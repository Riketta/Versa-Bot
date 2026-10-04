# LoL leaderboard plugin - data-source field guide

Maintainer notes for the [`deeplol.rs`](deeplol.rs) adapter: how to
explore and re-verify DeepLoL's public CDN API when something drifts.
Everything below was verified live (October 2026, game version 16.19).

## The service

- Root: `https://b2c-api-cdn.deeplol.gg` - a public FastAPI service, **no
  auth, no key**. DeepLoL's own website drives it from the browser.
- Self-describing: `GET /docs` (Swagger UI) and `GET /openapi.json` list
  every endpoint. When in doubt, read the spec - it is the ground truth
  for parameter names and requiredness.
- Health: `GET /common/deeplol-status` -> `{"status":true,...}`.
- The service is fine with plain HTTP clients; a browser-ish
  `User-Agent` is polite but not required. Rate limiting is
  unspecified - we pace requests sequentially with a configurable
  interval and expect nothing.

## The leaderboard endpoint

`GET /summoner/summoner_rank?platform_id={region}&lane=All&page={n}`

Gotchas, all live-verified:

- **`platform_id` is Riot's platform id**, digit-suffixed for every
  region except Korea: `KR`, `EUW1`, `EUN1`, `NA1`, `JP1`, `BR1`,
  `TR1`, `TW2`, `VN2`, `SG2` (the `*2` codes are the post-Garena
  servers; `SEA` maps to `SG2`). Digitless guesses like `EUW`, `NA` or
  `KR1` do **not** 422 - they fail inside the handler with
  `{"msg":"Internal Server Error"}`, which looks like an outage but is
  a bad key. Each of these codes was probed live (October 2026):
  exactly the set above answers 200, every digitless / `KR1` variant
  answers 500. Same trap for `lane`: valid values are `All`, `Top`,
  `Jungle`, `Middle`, `Bot`, `Supporter` (case-insensitive); `MID`,
  `BOTTOM`, `UTILITY`, `SUP` all 500.
- **`lane=All` is all you need**: every entry already carries the
  player's `most_role` and `most_champion` list, so role distribution
  and champion tables come from one sweep - never query per lane.
- **Page size is 100 players.** The answer carries `total_page`; pages
  beyond it 500. Clamp pagination to it - requesting more than exists
  is an error, not an empty page. The bot additionally stops on an
  empty page and at a computed page cap (depth / 10 + 10), so a
  degraded board can never keep the walk alive.
- A missing required parameter is the one clean failure: FastAPI
  answers `422` with a `detail` array. If you get `422`, read it - it
  names the missing parameter.
- Per-entry fields we consume (all defaulted in our structs, so added
  or removed fields degrade to defaults - a wrong-typed field still
  fails that page and errors the region for the cycle): `rank` (sparse
  - hidden players create gaps; we re-number positions densely after
  sorting),
  `most_role` (always a concrete lane in practice, no "All" players),
  `most_champion` (ordered id list, first = most played; can be
  missing/empty).
- The answer also carries pre-computed ratios (`rate_top`, ...), tier
  cut-offs and LP curves. We deliberately ignore them: we need
  bucket-controlled distributions (TOP 300 / TOP 1000 slices), which
  only our own aggregation gives.

## Champion names

`GET /common/champion-info` -> `{"champions": [{"champion_id": "1",
"champion_name_en": "Annie", "champion_name_kr": "...", "image_name":
"..."}]}`. Current game version, no auth. We cache it with the same TTL
as leaderboard data and degrade unknown ids to `Champion #id`.

## Quick curl check

```bash
curl -s "https://b2c-api-cdn.deeplol.gg/summoner/summoner_rank?platform_id=EUW1&lane=All&page=1" | head -c 400
```

Check a digit-suffixed code here, not just `KR` - `KR` is the one
region whose platform id is digitless, so a "works for KR" probe can
hide a wrong digitless convention for everything else. If this 500s,
re-check the query values against the lists above before assuming an
outage - the 500-on-bad-enum quirk makes typos look like server fires.
`/common/deeplol-status` tells a real outage from a bad request in one
call.
