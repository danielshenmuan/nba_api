# Data Sources & Lineage

**Last verified:** 2026-09-26 (live against GCP + the deployed API)

---

## Lineage

```
SportsBlaze API  ──► Cloud Run Job ──► BigQuery ──► Cloud Run API ──► n8n ──► Discord ──► Threads / X
  (daily boxscores)   nba-daily-ingest   nba_data     nba-gbq-api

Yahoo Fantasy    ──► Cloud Run Job ──► BigQuery
  (roster %)          nba-ownership-ingest

Yahoo Fantasy    ──► league-history (Next.js on Vercel)  [direct, OAuth per user, no DB]
```

---

## Upstream sources

| Source | Endpoint | Auth | Cadence | Writes to | Status |
|---|---|---|---|---|---|
| **SportsBlaze** | `api.sportsblaze.com/nba/v1/boxscores/daily` | `?key=` — **API key expires monthly, manual re-request** | Daily 05:00 UTC | `player_daily_game_stats_p` | ✅ Running |
| **Yahoo Fantasy** | via `yfpy` | OAuth2 `fspt-r` | Daily 06:00 UTC | `player_ownership` | 🔴 **Failing since 2026-02-02 — auth error** |
| **Yahoo Fantasy** | REST v2 | OAuth2 `fspt-r`, per-user | On request | nothing (cached 12h) | ✅ Running |
| ~~stats.nba.com~~ | ~~`nba_api`~~ | none | — | — | ⛔ **Blocked since 2026-01-24.** Returns HTTP 000 / timeout. 139 consecutive failures. Do not revive |

### ⚠️ Player ID namespaces

**SportsBlaze uses UUIDs. Everything else uses NBA integer player_id.**

| Source | Jokić |
|---|---|
| SportsBlaze | `9e1eebff-3f03-5baa-95aa-1a98aebaa0b0` |
| NBA / Yahoo ownership / historical | `203999` |

Bridged by `nba_data.player_id_crosswalk`. **Always read daily stats through `v_player_daily_game_stats`, never the raw `_p` table.**

---

## BigQuery — `fantasy-survivor-app.nba_data` (location `northamerica-northeast1`)

> ⚠️ Cloud Run is `us-central1`. Every query is cross-region.

| Table | Rows | Range | Keyed by | Notes |
|---|---|---|---|---|
| `player_daily_game_stats_p` | 5,976 | 2026-02-01 → 05-10 | **SB UUID** | Live SportsBlaze target. Partitioned `game_date`, clustered `(player_id, season)` |
| `player_daily_game_stats` | 41,607 | 2024-10-22 → **2026-01-23** | NBA id | Frozen. Pre-SportsBlaze |
| `player_daily_game_stats_p_backup` | 41,774 | 2024-10-22 → 2026-01-23 | NBA id | Pre-migration backup |
| `player_historical_game_stats` / `_p` | 125,463 each | 2024-10-22 → 2025-04-13 | NBA id | **Byte-identical duplicates — drop one** |
| `player_historical_game_stats_zscore` | 95,248 | — | NBA id | |
| `player_ownership` | 67,289 | 2025-10-27 → **2026-02-02** | NBA id | Stale. Cannot be backfilled |
| `league_pg_stats_by_season` | 1 | season `2025-26` | — | League means/stdevs for z-scores |
| `stats_means_stdevs` | 1 | — | — | Legacy baselines |
| `nba_schedule` | 126 | — | — | |
| **`player_id_crosswalk`** | **532** | — | both | **SB UUID ↔ NBA id.** 494 exact_name, 38 unresolved, 0 ambiguous |

**Views:** `v_player_daily_game_stats` (ID-remapped, **use this one**), `v_player_ownership_latest`, `v_player_roster_pct_latest`, `player_games_next_7_days`

### 🔴 Known gap
**2026-01-24 → 01-31 exists in no table.** NBA blocked on Jan 24; SportsBlaze started Feb 1. Eight days permanently lost unless refetched from SportsBlaze.

---

## Serving — Cloud Run (`us-central1`)

| Service | Purpose | Status |
|---|---|---|
| `nba-gbq-api` | Public read API (FastAPI) | ✅ |
| `nba-api` | Older service | ❓ **Superseded? Audit and delete** |
| `nba-trigger-service` | Triggers jobs | ✅ |

### `nba-gbq-api` endpoints

| Endpoint | Reads | Status |
|---|---|---|
| `/health` | — | ✅ |
| `/v1/players_search` | crosswalk-independent | ✅ |
| `/v1/daily_leaders` | daily stats | ✅ (empty off-season) |
| `/v1/player_timeseries` | `v_player_daily_game_stats` | 🟡 Fixed in code, **needs redeploy** |
| `/v1/player_baselines` | view + `league_pg_stats_by_season` | 🟡 Fixed in code, **needs redeploy** |
| `/v1/player_roster_pct` | `v_player_ownership_latest` | 🟡 Serves stale Feb-2026 data |

## Jobs & schedules

| Job | Schedule | Runs | Status |
|---|---|---|---|
| `nba-daily-ingest` | `0 1 * * *` (Cloud Scheduler) | `daily_ingest_sportsblaze.py` | ✅ Completed 2026-09-26 05:04 |
| `nba-ownership-ingest` | `0 2 * * *` | `ownership_ingest.py` | 🔴 `exit(1)` — Yahoo auth |
| GitHub Actions `daily_ingest.yml` | `0 10 * * *` | `daily_ingest.py` ⛔ **NBA-based, will always fail** | **Disable or repoint** |
| ~~local crontab~~ | — | — | ✅ Removed 2026-09-26 |

---

## Secrets

**Never commit.** `.env`, `secrets.json`, `yahoo_oauth2.json` are gitignored as of 2026-09-26 — they were public for ~10 months. Production values belong in **Secret Manager**.

| Secret | Used by |
|---|---|
| `SPORTSBLAZE_API_KEY` | `nba-daily-ingest` (env var). Expires monthly |
| Yahoo consumer key/secret + refresh token | `nba-ownership-ingest`, league-history |
| GCP service account | GitHub Actions |

---

## Runbook

**Endpoint 500s** → check whether it reads `_p` directly instead of `v_player_daily_game_stats`.
**Ownership stale** → Yahoo token. Re-auth, re-run `nba-ownership-ingest`.
**No new daily stats in season** → check SportsBlaze key hasn't expired, then job logs.
**New player missing** → not in `player_id_crosswalk`. Add a row with `match_method='manual'`.
