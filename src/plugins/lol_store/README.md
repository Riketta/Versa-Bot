# Crawling the local League/Riot APIs

Field notes for exploring the League client's REST API (LCU) - the data
source behind this plugin. Everything below was verified against a live
client; where Riot reshuffles things between patches, the *method* still
holds even if individual paths move.

## Connecting

The client writes a lockfile next to its executable on every start:

```
<game install>/LeagueClientUx.exe -> lockfile at <game install>/lockfile
```

Format: `name:pid:port:token:protocol`, e.g.

```
LeagueClient:49044:38436:SyntheticTestTokenABC123xyz:https
```

- Base URL: `{protocol}://127.0.0.1:{port}` - the port is random per
  client start; **the token rotates with it**.
- Auth: HTTP basic, user `riot`, password = the lockfile token.
- TLS: self-signed certificate. `curl -k`, or
  `danger_accept_invalid_certs(true)` in reqwest.
- The client listens on loopback only - crawl from the same machine.

curl quickstart (git-bash):

```bash
IFS=':' read -r _ _ PORT TOKEN PROTO < "/e/Games/Riot Games/League of Legends/lockfile"
curl -sk -u "riot:$TOKEN" "https://127.0.0.1:$PORT/lol-summoner/v1/current-summoner"
```

A tiny python probe is often more comfortable (TLS bypass + JSON):

```python
import json, ssl, urllib.request, base64
lock = open(r"<game install>/lockfile").read().strip().split(":")
port, token = lock[2], lock[3]
ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
auth = base64.b64encode(f"riot:{token}".encode()).decode()
def get(path):
    req = urllib.request.Request(f"https://127.0.0.1:{port}{path}",
                                 headers={"Authorization": "Basic " + auth})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()   # error BODIES are worth reading!
```

## Discovery: `/help` is the map

The swagger endpoints are **disabled** by default
(`/swagger/v1/api-docs`, `/swagger/v2/swagger.json`,
`/swagger/v3/openapi.json` all 404). The self-documentation mechanism is
`/help`:

- `GET /help` - the whole schema: `{"events": {...}, "functions": {...},
  "types": {...}}` (~1400 functions, ~3400 types). Fetch once, filter
  names client-side; there is no server-side name search.
- `GET /help?target=<Name>` - details for one function or type: `url`,
  `http_method`, `arguments` (with optionality), `returns` - including
  full nested field definitions for types. This is the schema resolver.

Function names map mechanically onto paths: `GetLolStoreV1CatalogSales`
-> `GET /lol-store/v1/catalog/sales`. When unsure, resolve the name:

```python
print(get("/help?target=GetLolLootV1PlayerLootMap")[1])
# {"GetLolLootV1PlayerLootMap": {"http_method": "GET",
#  "url": "/lol-loot/v1/player-loot-map", ...}}
```

Two things `/help` will NOT show you:

1. **Static asset routes** (`/lol-game-data/assets/...`) are plain file
   serving, not RPC functions - they exist but are invisible to `/help`.
   Community knowledge or guessing by asset-path conventions is the only
   discovery path (`champion-summary.json`,
   `champions/{championId}.json`, ...).
2. **Webview-backed features** (purchase history, parts of the store UI)
   have no LCU REST endpoint at all - the client renders them via Riot's
   web store. If `/help` has no function for it, diffing snapshots is the
   workaround (this plugin's approach).

## The vector-parameter trap

LCU query binding takes **vector params as URL-encoded JSON arrays**.
Every other encoding 400s with a misleading message:

```
?inventoryTypes=CHAMPION&inventoryTypes=CHAMPION_SKIN   -> 400 (not a collection)
?inventoryTypes=CHAMPION,CHAMPION_SKIN                  -> 400 (not a collection)
?inventoryTypes[0]=CHAMPION                             -> 400 (unknown argument)
?inventoryTypes=["CHAMPION","CHAMPION_SKIN"]  URL-encoded  -> 200
```

Always URL-encode the JSON (the quotes matter): in curl use
`--data-urlencode` with `-G`, in python `urllib.parse.quote('["A","B"]')`.

Related: **read the 400 error bodies** - they are JSON and diagnose
precisely (`"Couldn't assign value to 'inventoryTypes' of type vector
because the input not a collection."`, `"Unknown argument 'x[0]' for
'GetLolStoreV1Inventory'"`, 404 `RESOURCE_NOT_FOUND` for wrong paths).
The error text is the fastest way to a correct request shape.

## Verified endpoint cheat sheet

Live-probed on patch ~26.x (EUW, 2026-10). Resolve anything that moved
via `/help?target=`.

Identity & assets:

| Path | Notes |
|---|---|
| `/lol-summoner/v1/current-summoner` | `gameName`, `tagLine`, `puuid`, level |
| `/lol-game-data/assets/v1/champion-summary.json` | id -> name map (no skins) |
| `/lol-game-data/assets/v1/champions/{championId}.json` | full data incl. `skins: [{id, name}]`; `championId = skinId / 1000` |
| `/lol-champions/v1/owned-champions-minimal` | ownership + `purchased` timestamps |

Store (what this plugin uses):

| Path | Notes |
|---|---|
| `/lol-store/v1/catalog` | bare = full catalog (~9.5k items): `localizations.<locale>.name`, `prices[]`, `itemRequirements[]` (skin -> champion), `releaseDate` |
| `/lol-store/v1/catalog/{inventoryType}` | trap: the path variant requires an `itemIds` **vector**; prefer the bare form |
| `/lol-store/v1/catalog/sales` | active sales; the `sale.prices[].discount` field is dead (always `0.0`) - compute % off against the catalog's original price |
| `/lol-shoppefront/v1/stores` | the new store frontend's shelves; rotation stores carry `rotatingStoreMetadata` (`rotationCadence`, `currRotationStartTime`, `nextRotationStartTime`); Mythic Shop = `displayMetadata.shoppefront.id == "MYTHIC_SHOP"` with DAILY/WEEKLY/BIWEEKLY rotations |
| `/lol-shoppefront/v1/store-digests` | compact shelf summaries |
| `/lol-yourshop/v1/status` | Your Shop event: `hubEnabled`, `startTime`, `endTime` (placeholder dates while deactivated) |
| `/lol-yourshop/v1/offers` | the 6 personal offers (`skinName`, `discountPrice`, `expirationDate`); 404s unless the event is live |
| `/lol-store/v1/order-notifications` | transient pending-purchase notices, not history (usually empty) |

Account-shaped (verified, deliberately unused by this plugin - the
account is only the API key):

| Path | Notes |
|---|---|
| `/lol-inventory/v1/wallet` | vector param: `?currencyTypes=["RP","lol_blue_essence","lol_orange_essence"]`; `[]` returns an empty map - you must list currencies |
| `/lol-inventory/v1/signedWallet` | same params; per-currency JWT whose decoded payload contains the balances - cheap change detection |
| `/lol-inventory/v1/inventory` | `?inventoryTypes=["CHAMPION","CHAMPION_SKIN",...]`: `itemId`, `purchaseDate`, `owned`, `rental`, `payload.isVintage` |
| `/lol-inventory/v1/signedInventory/simple` | one JWT over the whole inventory - ideal poll trigger |
| `/lol-loot/v1/player-loot-map` | **dashes** (`player-loot-map`, not `playerlootmap`); chest/key/shard counts |

## Gotchas summary

- Token and port rotate on every client start - re-read the lockfile, do
  not cache credentials across restarts (mid-restart polls race the file
  rewrite; retry once after a re-read).
- `/help` itself 400s on `?encoding=utf8&target=...` combos - use bare
  `/help` or `/help?target=<Name>`.
- JSON error bodies, not status codes alone, carry the diagnosis.
- Static assets are undocumented; webview features are absent. Community
  schema dumps (e.g. per-patch "League of Legends API" archives) mirror
  `/help` offline and are handy when the client is closed - a live
  `/help` fetch supersedes them.
- `LcuClient` (this plugin, `src/plugins/lol_store/lcu.rs`) implements
  all of the connection handling already - new endpoints are one trait
  method plus a DTO away.
