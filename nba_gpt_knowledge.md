# NBA GPT – API Quick Guide

**Base URL:** https://nba-gbq-api-896368614747.us-central1.run.app

## Endpoints (Actions)
### GET /daily_leaders  (operationId: getDailyLeaders)
**Query params**
- `game_date` (YYYY-MM-DD, required)
- `limit` (int, default 10, max 50)
- `mode` ("best" | "worst", default "best")
- `min_minutes` (int, default 20)

**Usage**
- Rankings by z-score sum on a date.
- For "worst" results, keep `min_minutes` to filter garbage time.

### GET /player_timeseries  (operationId: getPlayerTimeSeries)
**Query params**
- `player_id` (repeatable; e.g., `?player_id=2544&player_id=201939`) – **required**
- `start_date`, `end_date` (YYYY-MM-DD, required)
- `limit` (optional; cap rows per player)

**Usage**
- Returns per-game stats and z-scores per player in range.
- Good for comparisons; group by `player_id`.

## Z-score model (9-cat)
Categories: **PTS, REB, AST, STL, BLK, FG3M, FG%, FT%, TOV**.  
`z_total = z_PTS + z_REB + z_AST + z_STL + z_BLK + z_FG3M + z_FG% + z_FT% - z_TOV`.

> `min_minutes` default = **20** to reduce noise.

## Typical calls
- Leaders (best 5):  
  `/daily_leaders?game_date=2025-01-02&limit=5&mode=best&min_minutes=20`
- Leaders (worst 10, stricter minutes):  
  `/daily_leaders?game_date=2025-01-02&limit=10&mode=worst&min_minutes=25`
- Two-player comparison in December 2024:  
  `/player_timeseries?player_id=2544&player_id=201939&start_date=2024-12-01&end_date=2024-12-31`

## Common player IDs (handy)
- LeBron James **2544**
- Stephen Curry **201939**
- Kevin Durant **201142**
- Nikola Jokić **203999**
- Joel Embiid **203954**
- Giannis Antetokounmpo **203507**
- Anthony Davis **203076**
- Damian Lillard **203081**
- Kyrie Irving **202681**
- James Harden **201935**
- Jayson Tatum **1628369**
- Devin Booker **1626164**
- Luka Dončić **1629029**
- Kawhi Leonard **202695**
- Shai Gilgeous-Alexander **1628983**
- Jalen Brunson **1628973**
- Tyrese Haliburton **1630169**

> If a name isn’t listed, ask the user for the **player_id**. (Future improvement: add `/players_search`.)

## Response shape notes
- `/daily_leaders` returns an array of leaders with `player_id`, `player_name`, `team_abbr`, `minutes`, `z_total`, and `z_breakdown`.
- `/player_timeseries` returns `players: [{ player_id, player_name?, series: [{ game_date, team_abbr, opp_abbr, minutes, stats{…}, z_scores{…} }]}]`.
- API sends `Cache-Control: public, max-age=600` on reads. Treat data as fresh enough for chat answers.

## Error handling
- Bad dates → 400. Fix to `YYYY-MM-DD`.
- Empty results → likely no games/injury/off-range; suggest nearest game date.
- Never invent IDs or stats—ask the user to confirm IDs if ambiguous.
