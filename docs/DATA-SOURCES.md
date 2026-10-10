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

## ESPN pipeline (added 2026-10-09) — **cut over 2026-10-09**

ESPN's **unofficial** site API (the JSON its own site and app call). No SLA. Disney's terms prohibit scripted access, so: tiny volume (~15 requests/day), cache, honest user agent, never retry on 403, alert on any block. SportsBlaze stays live as validator and fallback until cutover.

| Piece | Where |
|---|---|
| Ingest | `jobs/espn_ingest.py` (`games`, `ownership`, `daily`, `seed-universe`, `repair-ids`) |
| Health check | `jobs/healthcheck.py` — exits non-zero on failure; optional Discord webhook |
| Deploy | `jobs/deploy_espn_jobs.sh` — creates `nba-espn-ingest` (02:30 ET) and `nba-api-healthcheck` (08:00 ET) |
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

**Cutover (done 2026-10-09):**
- `v_player_daily_game_stats` now reads `player_daily_game_stats_espn` with the **identical 35-column contract** (`game_id`/`team_id` cast to STRING, unresolved players included with negative ids). The n8n call `/v1/daily_leaders` returns identical players, scores, keys and types before and after.
- `league_pg_stats_by_season` is rebuilt **one season at a time** from the ESPN table (the old SQL replaced the whole table, so a new season erased the last one). The ESPN daily job refreshes the current *and previous* season each night. Rollback copy: `league_pg_stats_by_season_bak_20261009`.
- **Bug fixed:** FG%/FT% z-scores in `/v1/player_baselines` were NULL (then 0) because shot attempts came from `player_historical_game_stats_p`, which never contains the current season. They now come from the daily table.
- **Fallback to SportsBlaze:** run `infra/bq/ddl/nba_data.v_player_daily_game_stats.sportsblaze_fallback.sql`. The SportsBlaze job still runs and still writes `_p`.
- **API dependencies are now pinned** (`api/requirements.txt`, the exact set of the last good image). An unpinned rebuild pulled `pyarrow` 26 next to `numpy` 1.26 and broke three endpoints; it was rolled back within minutes. Deploy API changes with `--no-traffic --tag` and test before promoting.

**`z_score` scale (changed 2026-10-09):** the per-game `z_score` now counts turnovers at **0.2** (was 1.0). Last season's 26,547 rows were recomputed in place; the ingest (`TO_WEIGHT` in `espn_ingest.py`) and the SportsBlaze fallback view both use 0.2. Backup: `player_daily_game_stats_espn_bak_20261009`. The SportsBlaze job still *stores* `z_score` at weight 1 in `_p`; only its fallback view recomputes it. The health check treats a stale SportsBlaze table as a *warning*, not a failure.

**Ownership (done 2026-10-09):** `v_player_ownership_latest` is now **Yahoo primary, ESPN fallback**: a player gets Yahoo's value if Yahoo's latest snapshot is at most 3 days old, otherwise ESPN's. Yahoo is frozen at 2026-02-02, so today everyone is served from ESPN. Same 4 columns and types, so no API change. It matters: `/v1/daily_leaders` filters the *worst performers* list to players owned at 20% or more, so stale ownership was shaping the 爆胎榜 post. `v_ownership_daily` lists every source side by side with a percentile rank per source. Rollback: `nba_data.v_player_ownership_latest.yahoo_only.sql`.

**`z_score` weight (2026-10-09):** the per-game `z_score` now counts turnovers at **0.2** (was 1.0), matching `DEFAULT_TO_WEIGHT` in the analyst engine. History was recomputed in place; rollback copy `player_daily_game_stats_espn_bak_20261009`. The SportsBlaze fallback view recomputes `z_score` at the same weight so the scale does not change if it is used.

**Pinned dependencies:** `jobs/requirements.txt` and `api/requirements.txt` are exact `pip freeze` sets from images that ran. Bump deliberately; for the API, deploy with `--no-traffic --tag` and test before promoting.
