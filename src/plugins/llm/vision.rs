//! Image recognition: attached images become text descriptions at capture
//! time. The bot never passes raw pixels into conversation contexts - the
//! chat model reads markdown-style descriptions baked into the records, so
//! any declared model works, prompts stay text-only, and costs stay bounded
//! (each image is described at most once, at capture, under size caps).
//!
//! Downloads go to the driving adapter's pinned trusted host (Discord's
//! CDN); guild input can never name another peer. Every failure mode is
//! best-effort: an undescribable image is recorded undescribed, never a
//! broken capture.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use super::completion_port::{
    ChatMessage, ChatRole, CompletionRequest, LlmCompletionPort, TokenUsage,
};
use super::model::GenParams;
use super::usage_total::{Sample, UsageTracker};

/// Built-in recognition prompt when the operator set no `[llm]
/// image_prompt`.
pub(crate) const DEFAULT_IMAGE_PROMPT: &str = "Describe this image concisely for a chat \
     transcript: the scene, notable objects and people, and any visible text verbatim.";

/// Whole-request timeout for image downloads from the platform CDN - the
/// same class as prompt-file fetches: interactive work on the capture path.
const IMAGE_FETCH_TIMEOUT_SECS: u64 = 30;

/// The pinned trusted host for platform CDN fetches - the same pin the
/// prompt-file path enforces. Attachment URLs are adapter-minted, so this
/// is defense in depth, not the primary trust boundary.
pub(crate) const DISCORD_CDN_PREFIX: &str = "https://cdn.discordapp.com/";

/// Hard cap on either decoded dimension: real photos stay far below it, a
/// decompression bomb (hundreds of megapixels from a small PNG) does not
/// get past the decoder. Combined with the crate's allocation limit, this
/// bounds the decode spike on the capture path.
const MAX_DECODE_DIMENSION: u32 = 8192;

/// One image awaiting description, filtered by the engine from a message's
/// attachments. `ext` rides along so records keep the capture-baked
/// placeholder extension even when recognition is off or the per-message
/// cap overflows (see `conversation::placeholder_ext`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSource {
    pub url: String,
    pub content_type: Option<String>,
    pub ext: Option<String>,
}

/// One description job: the recognition call's parameters, resolved by the
/// engine from plugin settings + the channel's image model override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageJob {
    /// Provider-qualified model ref (`provider/model`).
    pub model: String,
    pub prompt: String,
    pub max_side: u32,
    pub jpeg_quality: u8,
    pub max_source_bytes: u64,
}

/// Where one batch's usage lands: the engine's tracker plus the owning
/// guild. `None` in tests and fakes - usage tracking is observability,
/// never a description requirement.
pub struct UsageSink<'a> {
    tracker: &'a UsageTracker,
    guild_id: Option<u64>,
}

impl<'a> UsageSink<'a> {
    pub(crate) fn new(tracker: &'a UsageTracker, guild_id: Option<u64>) -> Self {
        Self { tracker, guild_id }
    }
}

/// Driven port: turn image sources into descriptions, in input order -
/// `None` marks an undescribed image. Injections keep the engine's capture
/// path testable without network or a vision endpoint. When a sink is
/// given, every completion's usage is recorded into it (endpoint-reported;
/// a completion without a usage report counts as a request only - image
/// tokens have no honest character-based estimate).
#[async_trait]
pub trait ImageDescriber: Send + Sync {
    async fn describe(
        &self,
        job: &ImageJob,
        images: Vec<ImageSource>,
        usage: Option<UsageSink<'_>>,
    ) -> Vec<Option<String>>;
}

/// Whether an attachment qualifies for recognition: image MIME types only
/// (the platform's content type is authoritative; unknown types are not
/// guessed from extensions). Animated formats decode to their first frame.
pub(crate) fn is_image_source(source: &ImageSource) -> bool {
    source.content_type.as_deref().is_some_and(|mime| mime.starts_with("image/"))
}

pub struct VisionService {
    completion: Arc<dyn LlmCompletionPort>,
    fetch: reqwest::Client,
}

impl VisionService {
    /// Builds the service with its CDN download client.
    ///
    /// # Panics
    /// Only if reqwest cannot build a client from purely static settings
    /// (TLS backend unavailable) - a process-level defect, not config.
    pub fn new(completion: Arc<dyn LlmCompletionPort>) -> Self {
        let fetch = reqwest::Client::builder()
            // The CDN host is prefix-checked per request; redirects must not
            // carry the fetch off the pinned host, so none are followed.
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(IMAGE_FETCH_TIMEOUT_SECS))
            .build()
            .expect("static image-fetch client config expected to build");
        Self { completion, fetch }
    }

    async fn describe_one(
        &self,
        job: &ImageJob,
        image: &ImageSource,
        usage: Option<&UsageSink<'_>>,
    ) -> Option<String> {
        match self.describe_one_inner(job, image).await {
            Ok((description, reported)) => {
                if let Some(sink) = usage {
                    // Endpoint numbers when reported; a request-only sample
                    // otherwise (image tokens cannot be estimated from
                    // chars). Recorded on the SUCCESS path only - a failed
                    // job never ran a completion, so it is not a request.
                    let sample =
                        reported.as_ref().map_or_else(Sample::unreported_request, Sample::reported);
                    sink.tracker.record(&job.model, sink.guild_id, sample);
                }
                Some(description)
            }
            Err(err) => {
                tracing::warn!(%err, "image recognition failed - recording undescribed");
                None
            }
        }
    }

    async fn describe_one_inner(
        &self,
        job: &ImageJob,
        image: &ImageSource,
    ) -> Result<(String, Option<TokenUsage>), VisionError> {
        let bytes = self.download(image, job.max_source_bytes).await?;
        let jpeg = resize_to_jpeg(&bytes, job.max_side, job.jpeg_quality)?;
        let request = CompletionRequest {
            model: job.model.clone(),
            messages: vec![ChatMessage {
                images: vec![super::completion_port::ImagePart {
                    mime: "image/jpeg".to_owned(),
                    data_base64: BASE64.encode(&jpeg),
                }],
                ..ChatMessage::text(ChatRole::User, job.prompt.clone())
            }],
            params: GenParams::default(),
        };
        let started = Instant::now();
        let response = self.completion.complete(request).await?;
        tracing::debug!(
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            usage = ?response.usage,
            "image recognition completion finished"
        );
        Ok((response.content.trim().to_owned(), response.usage))
    }

    async fn download(
        &self,
        image: &ImageSource,
        max_source_bytes: u64,
    ) -> Result<Vec<u8>, VisionError> {
        // Trust boundary, enforced (mirrors `/llm_set_prompt`): only the
        // platform CDN is ever fetched, whatever the payload claims.
        if !image.url.starts_with(DISCORD_CDN_PREFIX) {
            return Err(VisionError::UntrustedHost);
        }
        let response = self
            .fetch
            .get(&image.url)
            .send()
            .await
            .map_err(|err| VisionError::Download(err.to_string()))?;
        // With redirects unfollowed, a 3xx arrives here as-is - rejected
        // like any other non-success instead of its body being decoded.
        if !response.status().is_success() {
            return Err(VisionError::Download(format!("HTTP {}", response.status())));
        }
        Self::read_capped(response, max_source_bytes).await
    }

    /// Stream-reads at most `max_source_bytes` bytes: the cap aborts the
    /// transfer mid-flight, so a missing or lying Content-Length cannot
    /// make the handler buffer a whole oversized body.
    async fn read_capped(
        mut response: reqwest::Response,
        max_source_bytes: u64,
    ) -> Result<Vec<u8>, VisionError> {
        if let Some(length) = response.content_length()
            && length > max_source_bytes
        {
            return Err(VisionError::TooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) =
            response.chunk().await.map_err(|err| VisionError::Download(err.to_string()))?
        {
            body.extend_from_slice(&chunk);
            if body.len() as u64 > max_source_bytes {
                return Err(VisionError::TooLarge);
            }
        }
        Ok(body)
    }
}

#[async_trait]
impl ImageDescriber for VisionService {
    async fn describe(
        &self,
        job: &ImageJob,
        images: Vec<ImageSource>,
        usage: Option<UsageSink<'_>>,
    ) -> Vec<Option<String>> {
        let mut descriptions = Vec::with_capacity(images.len());
        for image in &images {
            descriptions.push(self.describe_one(job, image, usage.as_ref()).await);
        }
        descriptions
    }
}

/// Rescales an image so its longest side fits `max_side` (aspect kept;
/// smaller images pass through) and re-encodes it as JPEG - the one wire
/// format every vision endpoint accepts, and the smallest post-resize
/// payload. GIF/WebP/animated input decodes to its first frame.
fn resize_to_jpeg(bytes: &[u8], max_side: u32, quality: u8) -> Result<Vec<u8>, VisionError> {
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|err| VisionError::Decode(err.to_string()))?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DECODE_DIMENSION);
    limits.max_image_height = Some(MAX_DECODE_DIMENSION);
    reader.limits(limits);
    let decoded = reader.decode().map_err(|err| VisionError::Decode(err.to_string()))?;
    let resized = if decoded.width().max(decoded.height()) > max_side {
        decoded.resize(max_side, max_side, image::imageops::FilterType::Triangle)
    } else {
        decoded
    };
    let mut jpeg = Vec::new();
    let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, quality);
    resized
        .to_rgb8()
        .write_with_encoder(encoder)
        .map_err(|err| VisionError::Encode(err.to_string()))?;
    Ok(jpeg)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VisionError {
    #[error("image host is outside the pinned platform CDN")]
    UntrustedHost,
    #[error("image download failed: {0}")]
    Download(String),
    #[error("image exceeds the configured size cap")]
    TooLarge,
    #[error("unsupported or undecodable image: {0}")]
    Decode(String),
    #[error("image encoding failed: {0}")]
    Encode(String),
    #[error(transparent)]
    Completion(#[from] super::completion_port::LlmError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::llm::completion_port::CompletionResponse;
    use crate::plugins::llm::completion_port::LlmError;

    fn source(mime: Option<&str>) -> ImageSource {
        ImageSource {
            url: "https://cdn.example.test/a.png".to_owned(),
            content_type: mime.map(str::to_owned),
            ext: None,
        }
    }

    #[test]
    fn only_image_mime_types_qualify() {
        assert!(is_image_source(&source(Some("image/png"))));
        assert!(is_image_source(&source(Some("image/jpeg"))));
        assert!(is_image_source(&source(Some("image/gif"))));
        assert!(!is_image_source(&source(Some("video/mp4"))));
        assert!(!is_image_source(&source(Some("text/plain"))));
        assert!(!is_image_source(&source(None)));
    }

    #[test]
    fn resize_keeps_small_images_and_shrinks_large_ones() {
        // 64x32 stays untouched; a 1024x512 portrait is fitted to 512 max side.
        let small = image::RgbImage::new(64, 32);
        let encoded = encode_png(&small);
        let jpeg = resize_to_jpeg(&encoded, 512, 85).expect("small image expected to encode");
        let decoded = image::load_from_memory(&jpeg).expect("jpeg expected to decode");
        assert_eq!((decoded.width(), decoded.height()), (64, 32));

        let large = image::RgbImage::new(1024, 512);
        let jpeg =
            resize_to_jpeg(&encode_png(&large), 512, 85).expect("large image expected to encode");
        let decoded = image::load_from_memory(&jpeg).expect("jpeg expected to decode");
        assert_eq!((decoded.width(), decoded.height()), (512, 256));
    }

    #[test]
    fn gif_decodes_to_its_first_frame() {
        let bytes = animated_gif();
        let jpeg = resize_to_jpeg(&bytes, 512, 85).expect("gif expected to encode");
        let decoded = image::load_from_memory(&jpeg).expect("jpeg expected to decode");
        assert_eq!((decoded.width(), decoded.height()), (2, 2));
    }

    #[test]
    fn undecodable_bytes_are_rejected_not_panicked_on() {
        let err = resize_to_jpeg(b"not an image", 512, 85).expect_err("garbage expected to fail");
        assert!(matches!(err, VisionError::Decode(_)));
    }

    /// The trust boundary is enforced at the fetch, not just by convention:
    /// a non-CDN URL fails before any network I/O - and before the
    /// completion port is ever touched.
    struct NeverCompletion;

    #[async_trait]
    impl LlmCompletionPort for NeverCompletion {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            panic!("host check must fail before the completion port is reached");
        }
    }

    #[tokio::test]
    async fn untrusted_host_is_rejected_without_any_fetch_or_call() {
        let service = VisionService::new(Arc::new(NeverCompletion));
        let job = ImageJob {
            model: "m".to_owned(),
            prompt: "p".to_owned(),
            max_side: 512,
            jpeg_quality: 85,
            max_source_bytes: 1000,
        };

        let out = service
            .describe(
                &job,
                vec![ImageSource {
                    url: "https://evil.test/a.png".to_owned(),
                    content_type: Some("image/png".to_owned()),
                    ext: None,
                }],
                None,
            )
            .await;

        assert_eq!(out, vec![None]);
    }

    #[test]
    fn pinned_cdn_prefix_is_the_only_accepted_shape() {
        assert!("https://cdn.discordapp.com/attachments/1/2/a.png".starts_with(DISCORD_CDN_PREFIX));
        assert!(!"https://cdn.discordapp.com.evil.test/a.png".starts_with(DISCORD_CDN_PREFIX));
        assert!(!"http://cdn.discordapp.com/a.png".starts_with(DISCORD_CDN_PREFIX));
    }

    /// Minimal one-shot TCP server (the provider-test pattern): swallows
    /// the request head, writes the scripted bytes, closes the connection.
    async fn raw_http_server(script: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            socket.write_all(&script).await.expect("write");
        });
        (format!("http://{addr}/a.png"), handle)
    }

    /// The streaming cap bounds memory on a close-delimited body (no
    /// Content-Length to pre-check): the transfer aborts once the cap is
    /// exceeded instead of buffering the whole body.
    #[tokio::test]
    async fn read_capped_aborts_oversized_close_delimited_body() {
        let script = format!("HTTP/1.0 200 OK\r\nConnection: close\r\n\r\n{}", "x".repeat(500));
        let (url, server) = raw_http_server(script.into_bytes()).await;
        let response = reqwest::get(&url).await.expect("fetch expected to succeed");

        let err = VisionService::read_capped(response, 64).await.expect_err("cap expected");
        let _ = server.await;

        assert!(matches!(err, VisionError::TooLarge));
    }

    #[tokio::test]
    async fn read_capped_passes_a_fitting_body_through() {
        let script = b"HTTP/1.0 200 OK\r\nConnection: close\r\n\r\npng-bytes".to_vec();
        let (url, server) = raw_http_server(script).await;
        let response = reqwest::get(&url).await.expect("fetch expected to succeed");

        let body = VisionService::read_capped(response, 64).await.expect("body expected");
        let _ = server.await;

        assert_eq!(body, b"png-bytes");
    }

    /// Redirects are unfollowed (`Policy::none` on the fetch client): a
    /// `302` surfaces AS the response for the download path to reject as a
    /// non-success (`VisionError::Download` carrying the status), and the
    /// Location target is never fetched. The single-accept fixture cannot
    /// serve a second request, so a followed redirect could not have
    /// produced this status. The status-mapping branch itself lives inside
    /// the CDN-pinned `download`, behind the host prefix check, which no
    /// local fixture URL can pass (the pin forces https; the fixture is
    /// plain TCP) - so this test pins the unfollowed-redirect precondition
    /// on the service's own client, the same seam the `read_capped` tests
    /// use below the host check.
    #[tokio::test]
    async fn redirect_responses_are_surfaced_not_followed() {
        let script = "HTTP/1.1 302 Found\r\nLocation: /moved.png\r\nContent-Length: 0\r\n\
             Connection: close\r\n\r\n"
            .to_owned();
        let (url, server) = raw_http_server(script.into_bytes()).await;
        // In-module construction keeps the service's REAL client - exactly
        // the Policy::none configuration the download path relies on.
        let service = VisionService::new(Arc::new(NeverCompletion));

        let response = service.fetch.get(&url).send().await.expect("redirect expected to surface");
        let _ = server.await;

        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
    }

    /// A lying `Content-Length` is rejected by the pre-check before any
    /// body byte is buffered: the streamed body here (4 bytes) would fit
    /// the cap, so only the header check can produce this `TooLarge`.
    #[tokio::test]
    async fn lying_content_length_is_rejected_before_the_body_streams() {
        let script =
            "HTTP/1.0 200 OK\r\nContent-Length: 9999\r\nConnection: close\r\n\r\ntiny".to_owned();
        let (url, server) = raw_http_server(script.into_bytes()).await;
        let response = reqwest::get(&url).await.expect("fetch expected to succeed");

        let err = VisionService::read_capped(response, 64)
            .await
            .expect_err("lying header expected to fail");
        let _ = server.await;

        assert!(matches!(err, VisionError::TooLarge));
    }

    /// Batch semantics of `describe`: results keep input order, each source
    /// is decided independently (a sibling failure never aborts the batch),
    /// and a rejected source never reaches the completion port - the
    /// panicking stub would explode the test if it did. The "one good
    /// source" half of the contract cannot be served hermetically THROUGH
    /// `describe`: the pinned CDN prefix forces an https URL, which a
    /// plain-TCP local fixture cannot answer (no TLS) - the same reason the
    /// `read_capped` tests call the private `read_capped` past the host
    /// check. The success side (exactly one completion call per good image,
    /// descriptions in input order) is therefore pinned at the seams that
    /// exist: the host/redirect/download tests here, and the engine-level
    /// capture tests, which drive the describer port with a fake.
    #[tokio::test]
    async fn describe_preserves_order_and_isolates_failures() {
        let service = VisionService::new(Arc::new(NeverCompletion));
        let job = ImageJob {
            model: "m".to_owned(),
            prompt: "p".to_owned(),
            max_side: 512,
            jpeg_quality: 85,
            max_source_bytes: 1000,
        };

        let out = service
            .describe(
                &job,
                vec![
                    ImageSource {
                        url: "https://evil.test/first.png".to_owned(),
                        content_type: Some("image/png".to_owned()),
                        ext: None,
                    },
                    ImageSource {
                        // Right host, wrong scheme: fails the same prefix
                        // check - proves the loop moves past a failed
                        // sibling instead of aborting the batch.
                        url: "http://cdn.discordapp.com/second.png".to_owned(),
                        content_type: Some("image/png".to_owned()),
                        ext: None,
                    },
                ],
                None,
            )
            .await;

        assert_eq!(out, vec![None, None]);
    }

    /// The usage sink records COMPLETED vision jobs only: an untrusted or
    /// failed image never ran a completion, so it must not inflate the
    /// request counter.
    #[tokio::test]
    async fn untrusted_host_records_no_usage() {
        use crate::kernel::spi_ports::PluginStoragePort;
        let storage = Arc::new(crate::test_support::InMemoryPluginStorage::new());
        let tracker = crate::plugins::llm::usage_total::UsageTracker::new(
            storage.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
        );
        let service = VisionService::new(Arc::new(NeverCompletion));
        let job = ImageJob {
            model: "m".to_owned(),
            prompt: "p".to_owned(),
            max_side: 512,
            jpeg_quality: 85,
            max_source_bytes: 1000,
        };

        let out = service
            .describe(
                &job,
                vec![ImageSource {
                    url: "https://evil.test/a.png".to_owned(),
                    content_type: Some("image/png".to_owned()),
                    ext: None,
                }],
                Some(UsageSink::new(&tracker, Some(7))),
            )
            .await;

        assert_eq!(out, vec![None]);
        let (all_time, _today) = tracker.guild_snapshot(7).await;
        assert_eq!(all_time.total.requests, 0, "a failed job is not a request");
    }

    fn encode_png(image: &image::RgbImage) -> Vec<u8> {
        let mut png = Vec::new();
        image
            .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
            .expect("png encoding expected to succeed");
        png
    }

    /// A real two-frame animated GIF, encoded by the same `gif` backend the
    /// decoder uses - the first-frame degradation is what's under test.
    fn animated_gif() -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut encoder = image::codecs::gif::GifEncoder::new(&mut buffer);
            encoder
                .encode_frame(image::Frame::new(image::RgbaImage::new(2, 2)))
                .expect("first frame expected to encode");
            encoder
                .encode_frame(image::Frame::new(image::RgbaImage::new(2, 2)))
                .expect("second frame expected to encode");
        }
        buffer
    }
}
