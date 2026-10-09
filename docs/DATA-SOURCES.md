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

---

## ESPN pipeline (added 2026-10-09) — deployed, **not yet cut over**

ESPN's **unofficial** site API (the JSON its own site and app call). No SLA. Disney's terms prohibit scripted access, so: tiny volume (~15 requests/day), cache, honest user agent, never retry on 403, alert on any block. SportsBlaze stays live as validator and fallback until cutover.

| Piece | Where |
|---|---|
| Ingest | `jobs/espn_ingest.py` (`games`, `ownership`, `daily`, `seed-universe`, `repair-ids`) |
| Health check | `jobs/healthcheck.py` — exits non-zero on failure; optional Discord webhook |
| Deploy | `jobs/deploy_espn_jobs.sh` — creates `nba-espn-ingest` (02:30 ET) and `nba-healthcheck` (08:00 ET) |
| Endpoints | `site.api.espn.com/apis/site/v2/sports/basketball/nba/{scoreboard,summary}` · `lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{year}/segments/0/leaguedefaults/3?view=kona_player_info` |

| Table | Contents | Notes |
|---|---|---|
| `player_daily_game_stats_espn` | 34 columns, NBA-era types. Regular season only (ESPN season type 2) | `game_id` = ESPN event id. Minutes are whole numbers (ESPN does not publish seconds). `player_id` = NBA person id, or `-espn_id` if unresolved |
| `player_ownership_espn` | `player_id, player_name, roster_pct, snapshot_date` + `pct_change, pct_started, injury_status, espn_player_id` | ESPN exposes **current values only**: history starts 2026-10-09. A different population from Yahoo — correlated, not equal |
| `espn_player_map` | ESPN athlete id → NBA person id | Only resolved mappings are stored. Fix one with a plain `UPDATE`/`INSERT` (`method='manual'`) |
| `nba_player_universe` | ~5,100 NBA ids with every name spelling | Seeded from `nba_api` 1.11.4's offline list + BigQuery history. Rebuild when a newer `nba_api` ships; then run `repair-ids` |

**Identity rules:** names are normalised (accents, punctuation, Jr./III removed). Resolution prefers ids seen in our own history, then active players, then anyone. Two-word names are retried in reverse order (ESPN "Yang Hansen" = NBA "Hansen Yang"). Ambiguity is never guessed: those players keep a surrogate id until fixed.

**Validated, full 2025-26 regular season:** 26,547 lines, 1,230 games, every team at exactly 82. Against the NBA-era table (Oct 21 – Jan 23): 99.29% of 11,325 shared lines identical on every counting stat. Against SportsBlaze (Feb 1 – Apr 12): 98.85% of 4,353. The ~1% that differ are ±1 in one stat (post-game corrections). ESPN adds ~9.6k lines that those tables never ingested, so last season's history in the older tables was incomplete.

**Known differences:** minutes are whole numbers; sub-minute cameos show as `0` and are skipped; the NBA Cup final (competition type `CC`) and All-Star games are excluded (ESPN tags them season type 2, but they count in neither the 82 games nor fantasy scoring; the NBA-era table wrongly included the Cup final); `game_id` is the ESPN event id.

**Cutover checklist** (not done): (1) repoint `v_player_daily_game_stats` to `player_daily_game_stats_espn`; (2) repoint `infra/bq/sql/create_league_pg_stats_by_season.sql` and `jobs/sql/` — it reads `_p` directly, so z-score baselines stay on incomplete data until changed; (3) decide whether SportsBlaze keeps running as fallback.
