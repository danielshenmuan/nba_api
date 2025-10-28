# Repository Guide for Future Agents

This document captures the key conventions and shortcuts that help both you (the human maintainer) and future AI agents work effectively inside this project. Each section explains **why it matters for code generation or review** so it doubles as onboarding and a checklist for automated contributors.

## Project Overview (Why it helps)
- Snapshot of the domain: FastAPI service, daily ingestion job, and BigQuery analytics.
- Knowing the high-level architecture lets an agent narrow searches and propose changes in the right subsystem instead of scanning the whole repo.

### Key Components
1. `api/` — FastAPI app (`app.py`) plus service modules.
2. `jobs/` — Ingestion, backfill scripts, and deployment helpers.
3. `infra/` — BigQuery DDL + schema snapshots.
4. `tools/` — One-off validation scripts.

Understanding this layout reduces time spent discovering files and avoids accidental edits in unrelated areas.

## Coding Conventions (Why it helps)
- Prefer explicit imports and keep try/except blocks out of import statements.
- Use snake_case for functions/variables, PascalCase for classes, and follow Black-style formatting (already enforced across the repo).
- Reuse shared helpers (e.g., `jobs/boxscore_v3_utils.py`) before adding new logic.

These reminders ensure generated code stays consistent with the existing style, lowering review friction.

## Testing & Validation (Why it helps)
- Primary quick check: `python -m compileall <file_or_dir>` for syntax validation.
- End-to-end ingestion/backfill flows rely on BigQuery + NBA APIs; when network access is unavailable, mock or describe the manual test steps instead of attempting live calls.

Documenting fast checks encourages agents to run lightweight validations and explain any skipped integration tests.

## Deployment Notes (Why it helps)
- API deploy script: `./api/deploy_app.sh`
- Ingest deploy script: `./jobs/deploy_daily_ingest.sh`

Mentioning the scripts keeps automated contributors from rewriting deployment steps and reminds them to update docs if the flow changes.

## BigQuery Schema Guidance (Why it helps)
- The canonical source for table layouts lives under `infra/bq/ddl/` and `infra/bq/schema/`.
- Any ingestion change that touches columns should update both the DDL and schema JSON to stay in sync.

Calling this out prevents schema drift and signals to agents that column tweaks require touching multiple files.

## PR & Commit Expectations (Why it helps)
- Summaries should focus on behavioral changes, not just file edits.
- Include a testing section that references commands actually run (or explain why none were run).

Setting these expectations yields better final messages and aligns with downstream review automation.

---
Feel free to expand this file as new workflows emerge—keeping it up to date directly benefits future human and AI collaborators.
