## Analytics Runtime: Known Failure Classes

- `duplicate visible tags / different backend ids`:
  signals: `duplicate_tag_candidates_found=true`, `duplicate_tag_candidates=[...]`, `selected_tag_candidate_id`, `duplicate_retry_attempt`.
- `wrong route launch vs batch DSL launch`:
  signals: `execution_mode=static_profile` (unexpected), `source_of_filter_value=static_profile`, no `parsed_sheet_tasks_count`.
- `stale parse false negative after successful apply`:
  signals: `tag_verify_success=url_marker_after_apply` + `apply_confirmed_but_parse_suspicious=true`.
- `writer accepted but nothing persisted`:
  signals: `planned_writes>0` with `validated_writes=0` or `validation_failed_cells>0`.

## Analytics Runtime: Golden Commands

- Dry-run batch DSL:
  `python -m src.run_profile_analytics --report-id analytics_tag_layout_example --browser-backend openclaw_cdp --tag-selection-mode script --writer-layout-api-batch-from-sheet-dsl-dry-run`
- Live-run batch DSL:
  `python -m src.run_profile_analytics --report-id analytics_tag_layout_example --browser-backend openclaw_cdp --tag-selection-mode script --writer-layout-api-batch-from-sheet-dsl`
- Narrow duplicate-tag debug run:
  `python -m src.run_profile_analytics --report-id analytics_tag_layout_example --browser-backend openclaw_cdp --tag-selection-mode script --writer-layout-api-target-dsl-cell A29 --writer-layout-api-batch-from-sheet-dsl`

## Analytics Runtime: Stop-Loss Rules

- Stop and do not debug tag layer further if logs show:
  - mass `goto_cell` / long cell scan loops,
  - wrong writer mode in final routing,
  - writer path mismatch (expected API batch, got UI/grid path).
- First fix routing/execution mode, then return to tag duplicate debugging.

## Analytics Runtime: Debug Artifacts to Save Before Any Risky Fix

- last command line
- `git status`
- current branch (`git rev-parse --abbrev-ref HEAD`)
- related runtime log file
- latest `layout_api_write_summary_*.json` / `*.txt`
- latest related right-panel/filter debug dumps from `exports/debug/`

## Update (2026-04-18): Duplicate Tag Candidates in amoCRM

- amoCRM popup can show visually identical tag suggestions with different internal IDs.
- Runtime now logs duplicate candidates as structured list:
  - `duplicate_tag_candidates_found`
  - `duplicate_tag_candidates=[{text,id,index}]`
  - `selected_tag_candidate_id`
  - `selected_tag_candidate_index`
- If apply URL confirms tag filter but parse is suspicious, runtime can retry the next duplicate candidate:
  - `apply_confirmed_but_parse_suspicious=true`
  - `duplicate_retry_attempt`
  - `duplicate_retry_exhausted`
  - `final_selected_tag_candidate_id`

## Update (2026-04-12): Google OAuth Auth Modes

Why system browser opens:
- Google OAuth desktop flow uses `InstalledAppFlow.run_local_server(...)`.
- This is independent from OpenClaw CDP and always launches the OS default browser when interactive auth is needed.

Current auth modes (`GOOGLE_API_AUTH_MODE`):
- `auto` (default): use cached token/refresh first; if unusable, allow interactive OAuth.
- `cache_only`: never open browser; fail fast if token is missing/invalid.
- `interactive_bootstrap`: explicit bootstrap mode for first-time token creation/refresh.

Recommended flow:
1. One-time bootstrap (explicit):
   - set `GOOGLE_API_AUTH_MODE=interactive_bootstrap`
   - run API inspector/write command once to create/update `token.json`.
2. Regular runs:
   - set `GOOGLE_API_AUTH_MODE=cache_only`
   - no unexpected OAuth popup browser.

Token/credentials storage:
- credentials: `credentials.json` (or `GOOGLE_API_CREDENTIALS_FILE`)
- token cache: `token.json` (or `GOOGLE_API_TOKEN_FILE`)
- refresh token is reused automatically when present.
## Update (2026-04-12): Anchor Targeting + Safer Discovery Stop

- Batch/isolated API layout modes now support exact DSL cell targeting:
  - `--writer-layout-api-target-dsl-cell A1|F1|...`
- Anchor ordering is deterministic by `(dsl_row, dsl_col)`.
- Discovery summary now includes `dsl_cell` per anchor.
- Discovery hard-limit is treated as a safety fuse only:
  - effective limit auto-raises above scan budget,
  - normal runs stop by `scan_range_exhausted` / configured structural reasons, not premature `cell_read_hard_limit`.
- Dry-run contract remains strict: `--writer-layout-api-batch-from-sheet-dsl-dry-run` never performs Google Sheets value updates.
## Update (2026-04-12): Generalized API Layout Discovery

- Discovery is now sheet-geometry based (not vertical-only):
  - scans metadata-bounded row/column bands,
  - collects DSL candidates across full scan range,
  - detects header blocks independently,
  - maps `DSL -> nearest valid table block` with row/column distance scoring.
- Supports blocks stacked vertically, side-by-side, and lower blocks after large gaps.
- Anchor payload now includes table bounds (`table_row_start/end`, `table_col_start/end`, `topology`).

## Batch Dry-Run Contract (Strict)

- `--writer-layout-api-batch-from-sheet-dsl-dry-run` performs discovery/parsing/scenario execution/write planning,
  but API writer is always called with `dry_run=true`.
- No real Google Sheets updates are allowed in this mode.

## Update (2026-04-12): Batch Dry-Run Contract

- `--writer-layout-api-batch-from-sheet-dsl-dry-run` now executes discovery + DSL parse + scenario execution + write planning,
  but calls API writer strictly with `dry_run=true`.
- Dry-run never sends Google Sheets `batchUpdate` value writes.
- Summary rows use `status=dry_run_planned` with `planned_updates`, and `updated_cells_count=0`.

## UTM Prefix Limitation (Current)

- `utm_source^=` is routed to `utm_prefix` handler.
- In current amoCRM UI route this remains best-effort deterministic entry/selection,
  not a guaranteed native prefix operator for all accounts/layouts.

## UTM Prefix Behavior (Current)

- `utm_source^=...` in batch DSL is routed to `utm_prefix` browser handler.
- Current UI path is **best-effort** and uses direct value entry/selection in available control.
- There is no guaranteed dedicated amoCRM UI operator for true prefix query in every account layout.
- If UI does not expose deterministic prefix semantics, runtime logs warning/failure explicitly.

## Update (2026-04-23): Deal Analyzer Call-First Pre-Limit Metadata Pass

`analyze-period` now runs an explicit lightweight call metadata pass **before** applying `--limit` and before heavy transcription work.

What it does:
- scans all period deals (after live refresh or fallback input source),
- collects lightweight call stats (counts, durations, recording/audio references, redial patterns),
- writes run artifacts:
  - `workspace/deal_analyzer/period_runs/<run_id>/call_pool_debug.json`
  - `workspace/deal_analyzer/period_runs/<run_id>/call_pool_debug.md`

New pre-limit aggregates are also stored in `summary.json`:
- `deals_total_before_limit`
- `deals_with_any_calls`
- `deals_with_recordings`
- `deals_with_long_calls`
- `deals_with_only_short_calls`
- `deals_with_autoanswer_pattern`
- `deals_with_redial_pattern`

## Update (2026-04-25): Call Review LLM Profiles

- Stable call-review real-write profile uses `qwen3.5:397b-cloud` as `ollama_model` (fallback: `deepseek-v3.1:671b-cloud`).
- `gemma4:26b` is experimental and dry-run only:
  - config: `workspace/tmp_tests/deal_analyzer/deal_analyzer.llm_gemma4_26b_experimental.json`
  - `deal_analyzer_write_enabled=false`
  - do not use this profile for battle write.
- For local gemma tests:
  - if `transcript_length_chars > 12000`, runtime routes directly to deepseek
  - local gemma uses fast timeouts (`preflight=60s`, `generation=240s`, `structured=240s`)
  - on gemma timeout runtime performs immediate fallback (`fallback_reason=main_timeout`).


## Update (2026-04-11): DSL Encoding + Date Normalization

- Google Sheets DSL discovery/routing now uses UTF-8 text as source-of-truth (no lossy mojibake repair conversions).
- Scenario execution normalizes date DSL values to canonical tokens before applying filters (`created/closed`, `all_time/...`).
- Date filter handler verifies normalized widget state and returns success when target state is already selected.

## Test Run Policy

Run tests via module invocation only:
`python -m pytest -q -p no:cacheprovider`

Smoke/regression subset:
`python -m pytest -q -p no:cacheprovider tests\test_analytics_flow_utm_exact.py tests\test_filter_handlers_v1.py tests\test_filter_registry_v1.py`

This guarantees the active project interpreter/venv is used and keeps discovery limited to configured test paths.


### Temp / Cache Hygiene

- Run tests via `python -m pytest ...` from project root so discovery follows `pytest.ini`.
- Do not use root-level scratch paths for tests (`tmp*`, `pytest-cache-files-*`).
- Keep temporary/debug artifacts under project-owned paths:
  - `exports/debug/` for runtime diagnostics
  - `workspace/` for local working files
- Ignore temporary directories in git (`exports/tmp*`, `tests/tmp*`, `.pytest_cache/`, `__pycache__/`, `*.pyc`).

# amoCRM + Google Sheets + OpenClaw/Ollama Automation (Local Skeleton)


## Runtime Update (2026-04-08)

Service target is unchanged:
`open analytics -> set filter -> capture all/active/closed -> write to sheet`.

Current MVP priority:
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

Confirmed working now:
- `source_kind=tag` via holder popup path
- filter apply
- capture `all/active/closed`
- compiled outputs (`compiled_profile`, `compiled_stage_pivot`)
- Google Sheets API write for first layout block
- last production markers: `tag_selection_success=true`, `Filter apply confirmed`, `successful_tabs=3/3`, `updatedCells=30`, `fallback used=false`

Not production-ready yet:
- `utm_exact` / `utm_prefix`
- batch-from-sheet-dsl production
- weekly refusals / AI summary / cleanup policy
## Current MVP Status (2026-04-08)

Service target: automate amoCRM report routine by profile:
`open screen -> set filter -> capture all/active/closed -> write to sheet`.

Current MVP priority:
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

Confirmed working right now:
- `source_kind=tag`
- tag selection via holder-popup path
- filter apply
- capture `all / active / closed`
- compiled outputs: `compiled_profile` and `compiled_stage_pivot`
- Google Sheets API writer for first layout block

Last confirmed production markers:
- `tag_selection_success=true`
- `Filter apply confirmed`
- `successful_tabs=3/3`
- `updatedCells=30`
- `fallback used=false`

Not production-ready yet:
- `utm_exact`
- `utm_prefix`
- batch-from-sheet-dsl production mode
- weekly refusals
- AI summary
- log retention/cleanup


Р В­РЎвЂљР С•РЎвЂљ Р С—РЎР‚Р С•Р ВµР С”РЎвЂљ РІР‚вЂќ Р В±Р ВµР В·Р С•Р С—Р В°РЎРѓР Р…РЎвЂ№Р в„– Р В»Р С•Р С”Р В°Р В»РЎРЉР Р…РЎвЂ№Р в„– Р С”Р В°РЎР‚Р С”Р В°РЎРѓ Р Т‘Р В»РЎРЏ Р С—Р С•РЎв‚¬Р В°Р С–Р С•Р Р†Р С•Р в„– Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘Р С‘ Р Р…Р В° Р Т‘Р С•Р СР В°РЎв‚¬Р Р…Р ВµР в„– Windows-Р СР В°РЎв‚¬Р С‘Р Р…Р Вµ.
Р СћР ВµР С”РЎС“РЎвЂ°Р С‘Р в„– РЎв‚¬Р В°Р С– Р Т‘Р С•Р В±Р В°Р Р†Р В»РЎРЏР ВµРЎвЂљ read-only MVP Р В±РЎР‚Р В°РЎС“Р В·Р ВµРЎР‚Р Р…Р С•Р С–Р С• РЎвЂЎРЎвЂљР ВµР Р…Р С‘РЎРЏ Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р С‘ amoCRM: РЎРѓР С”РЎР‚Р С‘Р С—РЎвЂљ Р С•РЎвЂљР С”РЎР‚РЎвЂ№Р Р†Р В°Р ВµРЎвЂљ Р С‘Р Р…РЎвЂљР ВµРЎР‚РЎвЂћР ВµР в„–РЎРѓ, РЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ РЎвЂљР ВµР С”РЎС“РЎвЂ°Р С‘Р Вµ РЎвЂ Р С‘РЎвЂћРЎР‚РЎвЂ№ Р С‘ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµРЎвЂљ РЎР‚Р ВµР В·РЎС“Р В»РЎРЉРЎвЂљР В°РЎвЂљ Р Р† `exports`.

## Р В­РЎвЂљР В°Р С—РЎвЂ№ РЎР‚Р ВµР В°Р В»Р С‘Р В·Р В°РЎвЂ Р С‘Р С‘

1. MVP Р В·Р В°Р С—Р С•Р В»Р Р…Р ВµР Р…Р С‘РЎРЏ Р В»Р С‘РЎРѓРЎвЂљР В° "Р Р†Р С•РЎР‚Р С•Р Р…Р С”Р В° Р С•РЎвЂљР С”Р В°Р В·Р С•Р Р†"
2. Weekly summary Р С—Р С• Р С•РЎвЂљР С”Р В°Р В·Р В°Р С
3. Р С’Р Р…Р В°Р В»Р С‘Р В· РЎРѓР Т‘Р ВµР В»Р С•Р С”, Р В·Р Р†Р С•Р Р…Р С”Р С•Р Р† Р С‘ Р С—РЎР‚Р ВµР В·Р ВµР Р…РЎвЂљР В°РЎвЂ Р С‘Р в„–

## Р В§РЎвЂљР С• РЎС“Р В¶Р Вµ Р ВµРЎРѓРЎвЂљРЎРЉ

- Р ВР В·Р С•Р В»Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р Р…Р В°РЎРЏ РЎРѓРЎвЂљРЎР‚РЎС“Р С”РЎвЂљРЎС“РЎР‚Р В° Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚Р С‘Р в„– Р Р†Р Р…РЎС“РЎвЂљРЎР‚Р С‘ `project`
- Р вЂР В°Р В·Р С•Р Р†Р В°РЎРЏ Р С”Р С•Р Р…РЎвЂћР С‘Р С–РЎС“РЎР‚Р В°РЎвЂ Р С‘РЎРЏ РЎвЂЎР ВµРЎР‚Р ВµР В· `.env`
- Р СџРЎР‚Р С•Р Р†Р ВµРЎР‚Р С”Р С‘ Р В±Р ВµР В·Р С•Р С—Р В°РЎРѓР Р…Р С•РЎРѓРЎвЂљР С‘ Р С—РЎС“РЎвЂљР ВµР в„– (Р В·Р В°Р С—РЎР‚Р ВµРЎвЂљ Р Р†РЎвЂ№РЎвЂ¦Р С•Р Т‘Р В° Р В·Р В° Р С—РЎР‚Р ВµР Т‘Р ВµР В»РЎвЂ№ Р С—РЎР‚Р С•Р ВµР С”РЎвЂљР В°)
- Р вЂєР С•Р С–Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ Р Р† Р С”Р С•Р Р…РЎРѓР С•Р В»РЎРЉ Р С‘ РЎвЂћР В°Р в„–Р В»
- Browser read-only MVP Р Т‘Р В»РЎРЏ amoCRM Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р С‘:
  - Playwright-РЎРѓР ВµРЎРѓРЎРѓР С‘РЎРЏ РЎРѓ `storage state`
  - Р В±Р С•Р В»РЎРЉРЎв‚¬Р С•Р Вµ Р С•Р С”Р Р…Р С• Р В±РЎР‚Р В°РЎС“Р В·Р ВµРЎР‚Р В° Р Т‘Р В»РЎРЏ РЎРѓРЎвЂљР В°Р В±Р С‘Р В»РЎРЉР Р…Р С•Р С–Р С• layout (`--start-maximized`, `no_viewport=True`)
  - РЎвЂЎРЎвЂљР ВµР Р…Р С‘Р Вµ РЎвЂљР ВµР С”РЎС“РЎвЂ°Р ВµР С–Р С• РЎРЊР С”РЎР‚Р В°Р Р…Р В° Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р С‘
  - DOM-debug Р Т‘Р В°Р СР С—РЎвЂ№ Р Т‘Р В»РЎРЏ Р С—Р С•Р Т‘Р В±Р С•РЎР‚Р В° РЎРѓР ВµР В»Р ВµР С”РЎвЂљР С•РЎР‚Р С•Р Р†
  - РЎРѓР С”РЎР‚Р С‘Р Р…РЎв‚¬Р С•РЎвЂљ + РЎРЊР С”РЎРѓР С—Р С•РЎР‚РЎвЂљ JSON/CSV Р Р† `exports`
- Р СџР С•Р Т‘Р С–Р С•РЎвЂљР С•Р Р†Р С‘РЎвЂљР ВµР В»РЎРЉР Р…РЎвЂ№Р в„– config-driven РЎРѓР В»Р С•Р в„–:
  - `config/page_profiles.yaml`
  - `config/report_profiles.yaml`
  - `config/table_mappings.yaml`
  - `src/config_loader.py`
- Р СџР С•РЎРѓРЎвЂљР С•РЎРЏР Р…Р Р…РЎвЂ№Р Вµ Р С—РЎР‚Р В°Р Р†Р С‘Р В»Р В° Р В°Р С–Р ВµР Р…РЎвЂљР Р…Р С•Р в„– РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р С‘ Р Р† `AGENTS.md`

## Р Р€РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С”Р В°

1. Р Р€РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С‘РЎвЂљРЎРЉ Python 3.11+.
2. Р РЋР С•Р В·Р Т‘Р В°РЎвЂљРЎРЉ Р С‘ Р В°Р С”РЎвЂљР С‘Р Р†Р С‘РЎР‚Р С•Р Р†Р В°РЎвЂљРЎРЉ Р Р†Р С‘РЎР‚РЎвЂљРЎС“Р В°Р В»РЎРЉР Р…Р С•Р Вµ Р С•Р С”РЎР‚РЎС“Р В¶Р ВµР Р…Р С‘Р Вµ.
3. Р Р€РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С‘РЎвЂљРЎРЉ Р В·Р В°Р Р†Р С‘РЎРѓР С‘Р СР С•РЎРѓРЎвЂљР С‘:
   `pip install -r requirements.txt`
4. Р Р€РЎРѓРЎвЂљР В°Р Р…Р С•Р Р†Р С‘РЎвЂљРЎРЉ Р В±РЎР‚Р В°РЎС“Р В·Р ВµРЎР‚ Р Т‘Р В»РЎРЏ Playwright:
   `python -m playwright install chromium`
5. Р РЋР С”Р С•Р С—Р С‘РЎР‚Р С•Р Р†Р В°РЎвЂљРЎРЉ `.env.example` Р Р† `.env` Р С‘ Р В·Р В°Р С—Р С•Р В»Р Р…Р С‘РЎвЂљРЎРЉ Р В·Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏ, Р С•РЎРѓР С•Р В±Р ВµР Р…Р Р…Р С•:
   - `AMO_BASE_URL`
   - `AMO_ANALYTICS_URL`
   - `AMO_VIEWPORT_WIDTH` / `AMO_VIEWPORT_HEIGHT` (Р Т‘Р В»РЎРЏ headless РЎР‚Р ВµР В¶Р С‘Р СР В°)

## Р СџР ВµРЎР‚Р Р†РЎвЂ№Р в„– РЎР‚РЎС“РЎвЂЎР Р…Р С•Р в„– Р В·Р В°Р С—РЎС“РЎРѓР С”

1. Р вЂ™ `.env` Р С—Р С•РЎРѓРЎвЂљР В°Р Р†Р С‘РЎвЂљРЎРЉ `AMO_HEADLESS=false`.
2. Р вЂ”Р В°Р С—РЎС“РЎРѓРЎвЂљР С‘РЎвЂљРЎРЉ reader РЎРѓ РЎР‚РЎС“РЎвЂЎР Р…Р С•Р в„– Р С—Р В°РЎС“Р В·Р С•Р в„–:
   `python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --wait-for-enter`
3. Р вЂ™ Р С•Р С”Р Р…Р Вµ Р В±РЎР‚Р В°РЎС“Р В·Р ВµРЎР‚Р В° Р С—РЎР‚Р С‘ Р Р…Р ВµР С•Р В±РЎвЂ¦Р С•Р Т‘Р С‘Р СР С•РЎРѓРЎвЂљР С‘ Р Р†Р С•Р в„–РЎвЂљР С‘ Р Р† amoCRM.
4. Р С›РЎвЂљР С”РЎР‚РЎвЂ№РЎвЂљРЎРЉ Р Р…РЎС“Р В¶Р Р…РЎвЂ№Р в„– РЎРЊР С”РЎР‚Р В°Р Р… Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р С‘.
5. Р вЂ™РЎвЂ№РЎРѓРЎвЂљР В°Р Р†Р С‘РЎвЂљРЎРЉ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚РЎвЂ№ Р С‘ Р Р†Р С”Р В»Р В°Р Т‘Р С”РЎС“ Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹.
6. Р вЂ™Р ВµРЎР‚Р Р…РЎС“РЎвЂљРЎРЉРЎРѓРЎРЏ Р Р† РЎвЂљР ВµРЎР‚Р СР С‘Р Р…Р В°Р В» Р С‘ Р Р…Р В°Р В¶Р В°РЎвЂљРЎРЉ Enter.
7. Reader РЎРѓРЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ РЎвЂљР ВµР С”РЎС“РЎвЂ°Р С‘Р в„– РЎРЊР С”РЎР‚Р В°Р Р… Р С‘ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р С‘РЎвЂљ screenshot + JSON/CSV.

## Р В РЎС“РЎвЂЎР Р…Р С•Р в„– Р В»Р С•Р С–Р С‘Р Р… Р С‘ РЎР‚РЎС“РЎвЂЎР Р…Р В°РЎРЏ Р С—Р С•Р Т‘Р С–Р С•РЎвЂљР С•Р Р†Р С”Р В° РЎРЊР С”РЎР‚Р В°Р Р…Р В°

Р вЂўРЎРѓР В»Р С‘ Р Р…Р Вµ РЎвЂ¦Р С•РЎвЂљР С‘РЎвЂљР Вµ, РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ РЎРѓР С”РЎР‚Р С‘Р С—РЎвЂљ Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘ Р С•РЎвЂљР С”РЎР‚РЎвЂ№Р Р†Р В°Р В» `AMO_ANALYTICS_URL`, Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р в„–РЎвЂљР Вµ `--skip-open`:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --skip-open --wait-for-enter`

## Р В Р ВµР С”Р С•Р СР ВµР Р…Р Т‘РЎС“Р ВµР СРЎвЂ№Р в„– Р С—РЎР‚Р В°Р С”РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– РЎР‚Р ВµР В¶Р С‘Р С (manual all-tab-modes)

Р вЂќР В»РЎРЏ Р В±Р В»Р С‘Р В¶Р В°Р в„–РЎв‚¬Р ВµР в„– РЎРѓРЎвЂљР В°Р В±Р С‘Р В»РЎРЉР Р…Р С•Р в„– РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂ№ Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р в„–РЎвЂљР Вµ Р С—Р С•Р В»РЎС“Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– РЎР‚Р ВµР В¶Р С‘Р С:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes-manual`

Р С™Р В°Р С” РЎРЊРЎвЂљР С• РЎР‚Р В°Р В±Р С•РЎвЂљР В°Р ВµРЎвЂљ:

- Р С—Р С•РЎРѓР В»Р Вµ Р С—Р ВµРЎР‚Р Р†Р С•Р С–Р С• Enter reader РЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ РЎвЂљР ВµР С”РЎС“РЎвЂ°Р С‘Р в„– РЎРЊР С”РЎР‚Р В°Р Р… Р С”Р В°Р С” `all` Р С‘ РЎРѓРЎР‚Р В°Р В·РЎС“ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµРЎвЂљ export;
- Р В·Р В°РЎвЂљР ВµР С Р С—РЎР‚Р С•РЎРѓР С‘РЎвЂљ Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹ Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР С‘РЎвЂљРЎРЉ Р Р†Р С”Р В»Р В°Р Т‘Р С”РЎС“ Р Р…Р В° `Р С’Р С™Р СћР ВР вЂ™Р СњР В«Р вЂў` Р С‘ Р Р…Р В°Р В¶Р В°РЎвЂљРЎРЉ Enter;
- РЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ `active` Р С‘ РЎРѓРЎР‚Р В°Р В·РЎС“ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµРЎвЂљ export;
- Р В·Р В°РЎвЂљР ВµР С Р С—РЎР‚Р С•РЎРѓР С‘РЎвЂљ Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹ Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР С‘РЎвЂљРЎРЉ Р Р†Р С”Р В»Р В°Р Т‘Р С”РЎС“ Р Р…Р В° `Р вЂ”Р С’Р С™Р В Р В«Р СћР В«Р вЂў` Р С‘ Р Р…Р В°Р В¶Р В°РЎвЂљРЎРЉ Enter;
- РЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ `closed` Р С‘ РЎРѓРЎР‚Р В°Р В·РЎС“ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµРЎвЂљ export.

Р вЂ™ РЎРЊРЎвЂљР С•Р С РЎР‚Р ВµР В¶Р С‘Р СР Вµ Р Р…Р ВµРЎвЂљ Р В°Р Р†РЎвЂљР С•Р С”Р В»Р С‘Р С”Р С•Р Р† Р С—Р С• Р Р†Р С”Р В»Р В°Р Т‘Р С”Р В°Р С, Р С—Р С•РЎРЊРЎвЂљР С•Р СРЎС“ Р С•Р Р… Р Р…Р В°Р Т‘Р ВµР В¶Р Р…Р ВµР Вµ Р С”Р В°Р С” workaround, Р С—Р С•Р С”Р В° auto-switching Р ВµРЎвЂ°Р Вµ Р Т‘Р С•РЎР‚Р В°Р В±Р В°РЎвЂљРЎвЂ№Р Р†Р В°Р ВµРЎвЂљРЎРѓРЎРЏ.

## Profile-driven analytics flow (Р Р…Р С•Р Р†РЎвЂ№Р в„– РЎв‚¬Р В°Р С–)

Р вЂќР С•Р В±Р В°Р Р†Р В»Р ВµР Р… Р С—Р ВµРЎР‚Р Р†РЎвЂ№Р в„– profile-driven РЎР‚Р ВµР В¶Р С‘Р С:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example`

Р В§РЎвЂљР С• Р Т‘Р ВµР В»Р В°Р ВµРЎвЂљ РЎР‚Р ВµР В¶Р С‘Р С:

- Р В·Р В°Р С–РЎР‚РЎС“Р В¶Р В°Р ВµРЎвЂљ report profile Р С‘Р В· `config/report_profiles.yaml`;
- Р С•РЎвЂљР С”РЎР‚РЎвЂ№Р Р†Р В°Р ВµРЎвЂљ `analytics_sales` РЎРЊР С”РЎР‚Р В°Р Р…;
- Р С—РЎвЂ№РЎвЂљР В°Р ВµРЎвЂљРЎРѓРЎРЏ Р С•РЎвЂљР С”РЎР‚РЎвЂ№РЎвЂљРЎРЉ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚ Р С‘ Р Р†РЎвЂ№РЎРѓРЎвЂљР В°Р Р†Р С‘РЎвЂљРЎРЉ `filter_source` (`tag` Р С‘Р В»Р С‘ `utm_source`) + `filter_values`;
- Р Р…Р В°Р В¶Р С‘Р СР В°Р ВµРЎвЂљ `Р СџРЎР‚Р С‘Р СР ВµР Р…Р С‘РЎвЂљРЎРЉ`;
- Р В·Р В°Р С—РЎС“РЎРѓР С”Р В°Р ВµРЎвЂљ capture Р Р†Р С”Р В»Р В°Р Т‘Р С•Р С” Р С—Р С• URL `deals_type=all/active/closed`;
- РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…РЎРЏР ВµРЎвЂљ JSON/CSV Р С—Р С• Р С”Р В°Р В¶Р Т‘Р С•Р в„– Р Р†Р С”Р В»Р В°Р Т‘Р С”Р Вµ.

Р СњР В° РЎРЊРЎвЂљР С•Р С РЎРЊРЎвЂљР В°Р С—Р Вµ РЎРЊРЎвЂљР С• Р С—Р ВµРЎР‚Р Р†РЎвЂ№Р в„– РЎв‚¬Р В°Р С– Р С” Р С—Р С•Р В»Р Р…Р С•Р СРЎС“ automation flow: `profile -> filter -> all/active/closed capture`.
Р вЂўРЎРѓР В»Р С‘ automation РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р В° Р Р…Р Вµ РЎРѓРЎР‚Р В°Р В±Р С•РЎвЂљР В°Р В» Р С‘Р В·-Р В·Р В° РЎРѓР ВµР В»Р ВµР С”РЎвЂљР С•РЎР‚Р С•Р Р†, РЎРѓР СР С•РЎвЂљРЎР‚Р С‘РЎвЂљР Вµ debug screenshots Р Р† `workspace/screenshots` Р С‘ debug dumps Р С—Р В°Р Р…Р ВµР В»Р С‘ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р В° Р Р† `exports/debug/` (`*_filter_panel_visible_text_*.txt`, `*_filter_panel_selectors_*.json`).

Р вЂќР С•Р С—Р С•Р В»Р Р…Р С‘РЎвЂљР ВµР В»РЎРЉР Р…Р С• Р Р†Р С”Р В»РЎР‹РЎвЂЎР ВµР Р… scroll-debug Р С—Р В°Р Р…Р ВµР В»Р С‘ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р В°: РЎРѓР С•Р В·Р Т‘Р В°РЎР‹РЎвЂљРЎРѓРЎРЏ Р С—Р С•РЎв‚¬Р В°Р С–Р С•Р Р†РЎвЂ№Р Вµ РЎвЂћР В°Р в„–Р В»РЎвЂ№ `*_filter_panel_scroll_step_XX.txt` Р С‘ Р С•Р В±РЎР‰Р ВµР Т‘Р С‘Р Р…Р ВµР Р…Р Р…РЎвЂ№Р в„– `*_filter_panel_scroll_merged.txt`, РЎвЂЎРЎвЂљР С•Р В±РЎвЂ№ РЎС“Р Р†Р С‘Р Т‘Р ВµРЎвЂљРЎРЉ Р С—Р С•Р В»Р Р…РЎвЂ№Р в„– РЎРѓР С—Р С‘РЎРѓР С•Р С” РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р С•Р Р† Р С—Р С•РЎРѓР В»Р Вµ Р С—РЎР‚Р С•Р С”РЎР‚РЎС“РЎвЂљР С”Р С‘.
## Compile Р Р†Р ВµРЎР‚РЎвЂ¦Р Р…Р ВµР С–Р С• Р В±Р В»Р С•Р С”Р В° (Р С—Р ВµРЎР‚Р Р†РЎвЂ№Р в„– writer РЎв‚¬Р В°Р С–)

Р СџР С•РЎРѓР В»Р Вµ РЎРѓР В±Р С•РЎР‚Р В° РЎвЂљРЎР‚Р ВµРЎвЂ¦ JSON (`all/active/closed`) Р СР С•Р В¶Р Р…Р С• РЎРѓР С•Р В±РЎР‚Р В°РЎвЂљРЎРЉ Р С–Р С•РЎвЂљР С•Р Р†РЎвЂ№Р в„– compiled CSV Р Т‘Р В»РЎРЏ Р Р†Р ВµРЎР‚РЎвЂ¦Р Р…Р ВµР С–Р С• Р В±Р В»Р С•Р С”Р В°:

`python -m src.run_compile_top_block`

Р В§РЎвЂљР С• Р Т‘Р ВµР В»Р В°Р ВµРЎвЂљ РЎРЊРЎвЂљР С•РЎвЂљ РЎв‚¬Р В°Р С–:

- РЎвЂЎР С‘РЎвЂљР В°Р ВµРЎвЂљ snapshot JSON Р Т‘Р В»РЎРЏ `all`, `active`, `closed` (Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘ Р В±Р ВµРЎР‚Р ВµРЎвЂљ Р С—Р С•РЎРѓР В»Р ВµР Т‘Р Р…Р С‘Р Вµ Р С‘Р В· `exports/`);
- Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“Р ВµРЎвЂљ `top_cards` Р С”Р В°Р С” Р С•РЎРѓР Р…Р С•Р Р†Р Р…Р С•Р в„– Р С‘РЎРѓРЎвЂљР С•РЎвЂЎР Р…Р С‘Р С”;
- РЎвЂћР С•РЎР‚Р СР С‘РЎР‚РЎС“Р ВµРЎвЂљ Р С—Р В»Р С•РЎРѓР С”Р С‘Р в„– CSV Р Р† `exports/compiled/`:
  - `stage_name`
  - `all_count`
  - `active_count`
  - `closed_count`
- Р ВµРЎРѓР В»Р С‘ РЎРЊРЎвЂљР В°Р С— Р С•РЎвЂљРЎРѓРЎС“РЎвЂљРЎРѓРЎвЂљР Р†РЎС“Р ВµРЎвЂљ Р Р† Р С•Р Т‘Р Р…Р С•Р в„– Р С‘Р В· Р Р†Р С”Р В»Р В°Р Т‘Р С•Р С”, РЎРѓРЎвЂљР В°Р Р†Р С‘РЎвЂљ `0`.

Р В­РЎвЂљР С• Р С—РЎР‚Р С•Р СР ВµР В¶РЎС“РЎвЂљР С•РЎвЂЎР Р…РЎвЂ№Р в„– Р С—РЎР‚Р В°Р С”РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– РЎв‚¬Р В°Р С– Р С—Р ВµРЎР‚Р ВµР Т‘ Р В·Р В°Р С—Р С‘РЎРѓРЎРЉРЎР‹ Р Р† РЎР‚Р ВµР В°Р В»РЎРЉР Р…РЎС“РЎР‹ РЎвЂљР В°Р В±Р В»Р С‘РЎвЂ РЎС“ (Google Sheets write Р С—Р С•Р С”Р В° Р Р…Р Вµ Р Р†РЎвЂ№Р С—Р С•Р В»Р Р…РЎРЏР ВµРЎвЂљРЎРѓРЎРЏ).

## Р С’Р Р†РЎвЂљР С•-Р С—РЎР‚Р С•Р С–Р С•Р Р… Р Р†РЎРѓР ВµРЎвЂ¦ Р Р†Р С”Р В»Р В°Р Т‘Р С•Р С” (URL-based)

Р СљР С•Р В¶Р Р…Р С• Р С—Р С•Р Т‘Р С–Р С•РЎвЂљР С•Р Р†Р С‘РЎвЂљРЎРЉ РЎРЊР С”РЎР‚Р В°Р Р… Р С•Р Т‘Р С‘Р Р… РЎР‚Р В°Р В· Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹ Р С‘ Р В·Р В°Р С—РЎС“РЎРѓРЎвЂљР С‘РЎвЂљРЎРЉ Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С•Р Вµ Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР ВµР Р…Р С‘Р Вµ Р Р†Р С”Р В»Р В°Р Т‘Р С•Р С”:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes`

Р вЂ™ РЎРЊРЎвЂљР С•Р С РЎР‚Р ВµР В¶Р С‘Р СР Вµ reader Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР В°Р ВµРЎвЂљ Р Р†Р С”Р В»Р В°Р Т‘Р С”Р С‘ РЎвЂЎР ВµРЎР‚Р ВµР В· URL-Р С—Р В°РЎР‚Р В°Р СР ВµРЎвЂљРЎР‚ `deals_type`, Р В±Р ВµР В· UI-Р С”Р В»Р С‘Р С”Р С•Р Р† Р С—Р С• Р Р†Р С”Р В»Р В°Р Т‘Р С”Р В°Р С:

- `deals_type=all`
- `deals_type=active`
- `deals_type=closed`

Р С™Р В°Р В¶Р Т‘Р В°РЎРЏ РЎС“РЎРѓР С—Р ВµРЎв‚¬Р Р…Р С• Р С—РЎР‚Р С•РЎвЂЎР С‘РЎвЂљР В°Р Р…Р Р…Р В°РЎРЏ Р Р†Р С”Р В»Р В°Р Т‘Р С”Р В° РЎРЊР С”РЎРѓР С—Р С•РЎР‚РЎвЂљР С‘РЎР‚РЎС“Р ВµРЎвЂљРЎРѓРЎРЏ РЎРѓРЎР‚Р В°Р В·РЎС“ (JSON + CSV).
Р вЂўРЎРѓР В»Р С‘ РЎвЂЎРЎвЂљР ВµР Р…Р С‘Р Вµ РЎРѓР В»Р ВµР Т‘РЎС“РЎР‹РЎвЂ°Р ВµР в„– Р Р†Р С”Р В»Р В°Р Т‘Р С”Р С‘ Р Р…Р Вµ РЎС“Р Т‘Р В°Р В»Р С•РЎРѓРЎРЉ, РЎС“Р В¶Р Вµ РЎРѓР С•РЎвЂ¦РЎР‚Р В°Р Р…Р ВµР Р…Р Р…РЎвЂ№Р Вµ РЎвЂћР В°Р в„–Р В»РЎвЂ№ Р С•РЎРѓРЎвЂљР В°РЎР‹РЎвЂљРЎРѓРЎРЏ Р Р† `exports/` Р С‘ Р Р…Р Вµ РЎвЂљР ВµРЎР‚РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ.

`--all-tab-modes-manual` Р С•РЎРѓРЎвЂљР В°Р ВµРЎвЂљРЎРѓРЎРЏ Р В·Р В°Р С—Р В°РЎРѓР Р…РЎвЂ№Р С РЎР‚Р ВµР В¶Р С‘Р СР С•Р С: Р С—Р С•Р В»РЎРЉР В·Р С•Р Р†Р В°РЎвЂљР ВµР В»РЎРЉ Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹ Р С—Р ВµРЎР‚Р ВµР С”Р В»РЎР‹РЎвЂЎР В°Р ВµРЎвЂљ Р Р†Р С”Р В»Р В°Р Т‘Р С”Р С‘ Р С‘ Р С—Р С•Р Т‘РЎвЂљР Р†Р ВµРЎР‚Р В¶Р Т‘Р В°Р ВµРЎвЂљ РЎв‚¬Р В°Р С–Р С‘ Enter.

## Р В Р В°РЎРѓРЎв‚¬Р С‘РЎР‚РЎРЏР ВµР СР В°РЎРЏ Р С”Р С•Р Р…РЎвЂћР С‘Р С–РЎС“РЎР‚Р В°РЎвЂ Р С‘РЎРЏ

Р ВР Т‘Р ВµРЎРЏ Р С—РЎР‚Р С•РЎРѓРЎвЂљР В°РЎРЏ:

- Р С”Р С•Р Т‘ = Р Т‘Р Р†Р С‘Р В¶Р С•Р С” РЎвЂЎРЎвЂљР ВµР Р…Р С‘РЎРЏ/Р С•Р В±РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р С‘;
- config = РЎвЂЎРЎвЂљР С• Р С‘Р СР ВµР Р…Р Р…Р С• Р В·Р В°Р С—РЎС“РЎРѓР С”Р В°РЎвЂљРЎРЉ Р С‘ Р С”РЎС“Р Т‘Р В° РЎРѓР С”Р В»Р В°Р Т‘РЎвЂ№Р Р†Р В°РЎвЂљРЎРЉ РЎР‚Р ВµР В·РЎС“Р В»РЎРЉРЎвЂљР В°РЎвЂљ.

Р вЂќР В»РЎРЏ РЎР‚РЎС“РЎвЂЎР Р…Р С•Р С–Р С• РЎР‚Р ВµР Т‘Р В°Р С”РЎвЂљР С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘РЎРЏ Р С‘РЎРѓР С—Р С•Р В»РЎРЉР В·РЎС“РЎР‹РЎвЂљРЎРѓРЎРЏ YAML-РЎвЂћР В°Р в„–Р В»РЎвЂ№ Р Р† `config/`:

- `page_profiles.yaml` РІР‚вЂќ Р С”Р В°Р С”Р С‘Р Вµ РЎвЂљР С‘Р С—РЎвЂ№ РЎРѓРЎвЂљРЎР‚Р В°Р Р…Р С‘РЎвЂ  amoCRM Р ВµРЎРѓРЎвЂљРЎРЉ Р Р† Р С—РЎР‚Р С•Р ВµР С”РЎвЂљР Вµ;
- `report_profiles.yaml` РІР‚вЂќ Р С”Р В°Р С”Р С‘Р Вµ Р С•РЎвЂљРЎвЂЎР ВµРЎвЂљРЎвЂ№ Р В·Р В°Р С—РЎС“РЎРѓР С”Р В°РЎвЂљРЎРЉ, РЎРѓ Р С”Р В°Р С”Р С‘Р СР С‘ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р В°Р СР С‘/Р Р†Р С”Р В»Р В°Р Т‘Р С”Р В°Р СР С‘/Р С‘РЎРѓРЎвЂљР С•РЎвЂЎР Р…Р С‘Р С”Р В°Р СР С‘;
- `table_mappings.yaml` РІР‚вЂќ Р С”РЎС“Р Т‘Р В° Р С—Р С‘РЎРѓР В°РЎвЂљРЎРЉ РЎР‚Р ВµР В·РЎС“Р В»РЎРЉРЎвЂљР В°РЎвЂљ (РЎвЂ Р ВµР В»Р ВµР Р†РЎвЂ№Р Вµ Р В±Р В»Р С•Р С”Р С‘/РЎР‚Р ВµР В¶Р С‘Р СРЎвЂ№ Р В·Р В°Р С—Р С‘РЎРѓР С‘) Р Р…Р В° РЎРѓР В»Р ВµР Т‘РЎС“РЎР‹РЎвЂ°Р С‘РЎвЂ¦ РЎРЊРЎвЂљР В°Р С—Р В°РЎвЂ¦.

Р вЂ™ Р В±РЎС“Р Т‘РЎС“РЎвЂ°Р ВµР С Р Р…Р С•Р Р†РЎвЂ№Р Вµ РЎвЂљР ВµР С–Р С‘, Р Р…Р С•Р Р†РЎвЂ№Р Вµ Р С•РЎвЂљРЎвЂЎР ВµРЎвЂљРЎвЂ№, Р Р…Р С•Р Р†РЎвЂ№Р Вµ РЎРѓРЎвЂљРЎР‚Р В°Р Р…Р С‘РЎвЂ РЎвЂ№ amoCRM (`analytics`, `deals`, `events`) Р СР С•Р В¶Р Р…Р С• Р В±РЎС“Р Т‘Р ВµРЎвЂљ Р Т‘Р С•Р В±Р В°Р Р†Р В»РЎРЏРЎвЂљРЎРЉ РЎвЂЎР ВµРЎР‚Р ВµР В· config Р В±Р ВµР В· Р С—Р ВµРЎР‚Р ВµР С—Р С‘РЎРѓРЎвЂ№Р Р†Р В°Р Р…Р С‘РЎРЏ РЎРЏР Т‘РЎР‚Р В°.

## Р вЂ™Р В°Р В¶Р Р…Р С• Р С—РЎР‚Р С• Р С•Р С–РЎР‚Р В°Р Р…Р С‘РЎвЂЎР ВµР Р…Р С‘РЎРЏ MVP

- Read-only Р С—Р С•Р Р†Р ВµР Т‘Р ВµР Р…Р С‘Р Вµ: Р Р…Р С‘Р С”Р В°Р С”Р С‘РЎвЂ¦ Р Т‘Р ВµР в„–РЎРѓРЎвЂљР Р†Р С‘Р в„– `save/submit/delete`.
- Р СњР В° РЎРЊРЎвЂљР С•Р С РЎв‚¬Р В°Р С–Р Вµ РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚РЎвЂ№ Р Р†РЎвЂ№РЎРѓРЎвЂљР В°Р Р†Р В»РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ Р Р†РЎР‚РЎС“РЎвЂЎР Р…РЎС“РЎР‹ Р Р† amoCRM UI.







## OpenClaw CDP Backend

?? ?? browser backend `openclaw_cdp`  ? ?  ?? OpenClaw-managed Chrome (?? CDP),  ?? ? ? Chromium ?.

 ?:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp`

?? ?? env:

- `BROWSER_BACKEND=openclaw_cdp`
- `OPENCLAW_CDP_URL=http://127.0.0.1:18800`

? CDP endpoint ?,   ??  ? ?  OpenClaw browser profile.


## External Agent Bridge (tag external_agent mode)

For `--tag-selection-mode external_agent` you can run an external bridge command after handoff JSON is prepared.

Example:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp --tag-selection-mode external_agent --external-agent-bridge-cmd "your_agent_bridge_command"`

Bridge environment variables:
- `EXTERNAL_AGENT_HANDOFF_PATH`
- `EXTERNAL_AGENT_TARGET_VALUE`
- `EXTERNAL_AGENT_URL_BEFORE`
- `EXTERNAL_AGENT_CDP_URL`

If bridge command is not set, flow falls back to manual Enter confirmation.


## Tag Selection In Script Mode (Enter + Esc)

For amoCRM tag filter in `script` mode the primary scenario is now keyboard-based:
1. Focus tag field
2. Type target tag
3. Press `Enter` to insert tag
4. Press `Esc` to close dropdown
5. Wait for `` button to become visible/enabled
6. Click ``
7. Verify URL contains `useFilter=y` and `tag[]`

Dropdown-item click path is kept only as fallback.


## Apply Step Reliability (amoCRM analytics filter)

When tag chip is already selected, flow now treats tag selection as successful and moves to apply-step.
Before click, reader scrolls filter panel to bottom, collects apply-button candidates, logs their debug payloads, and uses multi-step click fallback (`normal`, `scroll_then_click`, `force`, `bbox`, `js`).
Apply success is confirmed by URL/effect signals (URL changed or `useFilter` + `tag[]`).

## Writer MVP (Google Sheets UI test tab)

After successful profile capture, `run_profile_analytics` now also:

- builds one compiled result object for tabs `all/active/closed`;
- saves compiled JSON to `exports/compiled/`;
- opens Google Sheets in the same browser session;
- switches to test tab `analytics_writer_test`;
- clears the tab and writes fresh data from `A1`.

Writer MVP uses browser UI (no Google API credentials) and requires active user login in Google.

Configuration source:

- `config/table_mappings.yaml`
- mapping id: `analytics_writer_test_destination`
- fields: `sheet_url`, `tab_name`, `write_mode`, `start_cell`

You can also provide test sheet URL via env fallback:

- `GOOGLE_SHEETS_TEST_URL=...`

Run command:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example --browser-backend openclaw_cdp --tag-selection-mode script`


## Layout Writer (Anchor-Based, Non-Destructive)

A new writer mode is available: `kind: google_sheets_layout_ui`.

Goal:
- keep existing sheet formatting/merged cells/colors intact;
- find target block by text anchors;
- update only numeric cells (`all/active/closed`) for mapped stages.

How it works:
- build stage pivot from compiled result: `stage -> {all, active, closed}`;
- locate block anchor by alias list;
- locate header row (`stage/all/active/closed` aliases);
- build stage-row map;
- write planned numeric cells only.

Dry-run mode (no writes):

`python -m src.run_profile_analytics --report-id analytics_tag_layout_example --browser-backend openclaw_cdp --tag-selection-mode script --writer-layout-dry-run`

Raw writer path is preserved for debug/test tab (`kind: google_sheets_ui`).
## Browser Filters v1 (2026-04-09)

`src/browser/analytics_flow.py` now uses handler-based routing for supported browser filters.

Supported filters v1:
- `tag`
- `pipeline`
- `date`
- `manager`
- `utm_source` exact
- `utm_source` prefix (best-effort)

Implementation files:
- `src/browser/filters/base.py`
- `src/browser/filters/registry.py`
- `src/browser/filters/tag_filter.py`
- `src/browser/filters/utm_filter.py`
- `src/browser/filters/pipeline_filter.py`
- `src/browser/filters/date_filter.py`
- `src/browser/filters/manager_filter.py`

Out of scope for this step:
- writer/discovery/DSL redesign
- refusals/AI summary production flow
- universal selectors for every amoCRM tenant-specific UI variant

## Runtime Validation Status (2026-04-09)

Post-refactor status for browser filter handlers v1:
- Runtime verified: `tag`
- Runtime pending: `utm_source exact`, `pipeline`, `date`, `manager`
- `pipeline/date` are currently covered only through batch DSL scenarios as secondary filters.
- `manager` is not present in current runtime-ready profiles/DSL examples.

## Runtime MVP (Current)

Real MVP path today:
1. Browser/OpenClaw opens amoCRM analytics and applies UI filters.
2. Capture reads tabs `all / active / closed`.
3. Compiled artifacts are produced (`compiled_profile`, `compiled_stage_pivot`).
4. Google Sheets API writer updates target layout block (dry-run/live modes).

Supported `source_kind` status:
- `tag`: runtime-stable path.
- `utm_source` exact: implemented with strict row/popup-scoped runtime path; runtime stability still under active validation.

## Runtime Boundaries (Who Owns What)

- Orchestration:
  - `src/browser/analytics_flow.py`
  - Owns sequence: open panel -> select source -> apply filter -> apply button -> capture tabs.

- Filter runtime handlers:
  - `src/browser/filters/registry.py` routes by filter key.
  - `src/browser/filters/tag_filter.py` owns holder-popup tag selection + chip verify logic.
  - `src/browser/filters/utm_filter.py` owns UTM handler apply/verify wiring.
  - `src/browser/filters/pipeline_filter.py`, `date_filter.py`, `manager_filter.py` own v1 filter handler paths.

- Writer/discovery:
  - Kept separate from browser filter mechanics.
  - API layout discovery/write remains in writer/integration modules and is not part of filter runtime logic.

## UTM Exact Row-Scoped Logs

Row-scoped exact flow health signals:
- `utm_row_scope_resolved=true`
- `utm_row_multisuggest_id=...`
- `utm_popup_multisuggest_id=...`
- `utm_input_multisuggest_id=...`
- `utm_popup_id_matches_row=true`
- `utm_input_id_matches_popup=true`
- `utm_exact_selection_success=true`

If flow fails, inspect:
- `utm_exact_fail_reason=active_popup_not_opened`
- `utm_exact_fail_reason=row_scoped_input_not_activated`
- `utm_exact_fail_reason=chip_not_detected`

## Golden Runtime Commands

Tag x3:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`

UTM exact x2:
- `python -m src.run_profile_analytics --report-id analytics_utm_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`


## Writer Destination Diagnostics

When Google Sheets UI writer starts, logs now include destination context:
- `target_id`
- `sheet_url`
- `tab_name`
- `write_mode`

If tab is missing, runtime error includes `target_id`, `tab_name`, and a hint to verify `config/table_mappings.yaml`.
Writer also logs visible tab names detected in the sheet for faster troubleshooting.


## UTM Profile Routing (Legacy vs Layout)

Two UTM report profiles are intentionally separated:

- Legacy profile: `analytics_utm_single_example`
  - keeps legacy output target: `event_top_block_1`
- Layout writer profile: `analytics_utm_layout_example`
  - execution DSL source target: `analytics_layout_stage_blocks_destination`
  - writer output target: `analytics_layout_stage_blocks_destination`

Recommended commands for layout profile:

Dry-run:
`python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`

Real write:
`python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-write --browser-backend openclaw_cdp --tag-selection-mode script`


## Apply Fallback Reliability (2026-04-10)

`analytics_flow` apply path is hardened for UI pointer interception cases.

What changed:
- Added missing diagnostics helper `_dump_apply_button_diagnostics(...)` in `AnalyticsFlow`.
- Diagnostics are best-effort only and cannot crash runtime.
- `_click_apply_in_panel(...)` now confirms apply via short polling instead of one immediate check.
- Click strategy remains deterministic: `normal -> force -> js`.

Success confirmation for apply uses existing runtime signals:
- URL/filter marker confirmation,
- panel/overlay close,
- URL change after click.

## Config Hygiene (2026-04-10)

- Removed placeholder/mojibake-style `????????` value from `config/report_profiles.yaml` (`analytics_tag_layout_example`).
- Current profile config should no longer emit `suspicious_entries=['????????']` warning from config loader.
- Added regression test to guard report profile config against `???` placeholders.

## Batch DSL Execution Update (2026-04-10)

- Fixed batch scenario execution regression where `AnalyticsFlow` missed `_choose_option_text`.
- `utm_source^=` is now propagated as primary operator in scenario execution and routed through `utm_prefix` handler in browser flow.
- Non-primary filters in batch execution (`pipeline`, `date`, `manager`, secondary `tag`) are now strict:
  - if handler apply fails, scenario fails with controlled error (`Scenario filter apply failed: field=...`).
- Unsupported DSL fields now fail explicitly with controlled error (`Unsupported DSL filter for scenario execution: field=...`).

This removes silent/partial filter application in batch mode.

## Pipeline Batch Diagnostics (2026-04-10)

- Pipeline handler now uses row-scoped deterministic selection with explicit diagnostics.
- On failure, diagnostics include:
  - row container payload,
  - click target payload,
  - visible option texts,
  - option nodes count/payload,
  - selected value reflection status,
  - panel apply-button state.
- Batch scenario remains strict: pipeline apply failure causes controlled scenario failure.



## Weekly Refusals MVP (Events List)

Added separate runtime path for `source.page_type=events_list`.

- Browser flow: `src/browser/events_flow.py`
- Parser: `src/parsers/weekly_refusals_parser.py`
- Writer: `src/writers/weekly_refusals_block_writer.py`

This flow is independent from `analytics_sales` and does not reuse top-block parser logic.

### Profiles
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`

### Dry-run command
```bash
python -m src.run_profile_analytics --report-id weekly_refusals_weekly_2m --writer-layout-api-dry-run --browser-backend openclaw_cdp
```

### Runtime command
```bash
python -m src.run_profile_analytics --report-id weekly_refusals_weekly_2m --browser-backend openclaw_cdp
```

Artifacts:
- `exports/compiled/weekly_refusals_<report_id>_<timestamp>.json`
- `exports/debug/weekly_refusals_write_summary_<timestamp>.json`

## Weekly Refusals Profile IDs (Source of Truth)

Use these real report IDs:
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`
- `weekly_refusals_example` (alias/smoke profile, equivalent to `weekly_refusals_weekly_2m`)

### Smoke Dry-run
```bash
python -m src.run_profile_analytics --report-id weekly_refusals_example --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script
```

## Weekly Refusals: `event_type` Search Control Notes

- amoCRM field `РўРёРїС‹ СЃРѕР±С‹С‚РёР№` is rendered as `checkboxes-search` (not standard select/dropdown).
- Valid scope can be the control root itself (`filter__custom_settings__item checkboxes-search js-control-checkboxes-search`).
- Primary search-kind selectors:
  - open/check state: `.checkboxes-search__opening-list`, `.checkboxes-search__search-input`, `.checkboxes-search__section-common`, `.checkboxes-search__item-label`, `input[type='checkbox'][data-value]`
  - option resolve: `.checkboxes-search__item-label:has-text(...)`, `label:has(input[data-value='...'])`, `input[type='checkbox'][data-value='...']`
  - apply: `.js-checkboxes-search-list-apply` (including `.checkboxes-search__buttons-wrapper .button-input`) and `OK/РћРљ` variants.
- Do not use page-wide `label/li/input[type='checkbox']` for this stage: it can click left preset panel instead of opened `РўРёРїС‹ СЃРѕР±С‹С‚РёР№` widget.
- On failure, inspect `exports/debug/weekly_refusals_event_type_search_failed_<timestamp>.*`.
- Focus on JSON field `checkbox_search_debug_snapshot` (`active_element`, `control_scope_elements`, `ok_buttons`, `event_type_text_elements`).


## Update (2026-04-15): Anchor-Only Layout Writing + Skip Contract

### Analytics Layout Writer
- Block positioning uses discovered DSL/block/header anchors only.
- Runtime no longer hard-fails whole run when one block anchor is missing.
- Missing block behavior: `skipped` with detailed log (`block_name`, `aliases`, `reason`, debug dump/screenshot paths).
- If some blocks are found, they are processed independently.
- `start_cell` is not used as operational positioning source for `google_sheets_layout_ui`.

### Weekly Refusals Writer
- Anchor-based section discovery remains primary path.
- `allow_start_cell_fallback` still controls emergency fallback (default false for weekly blocks).
- If anchor is missing and fallback is disabled, writer emits explicit anchor diagnostics.

### Weekly Period Runtime Modes
Config/runtime now supports:
- `current_week`
- `previous_week`
- `auto_weekly` (mapped to monday-current-else-previous)
- `manual_range`

CLI overrides for weekly runs:
- `--weekly-period-strategy`
- `--weekly-period-mode`
- `--weekly-date-from`
- `--weekly-date-to`

### DSL Filter Support Boundary
Current scenario execution supports:
- `tags`
- `utm_source` (`=` and `^=`)
- `pipeline`
- `period`
- `dates_mode`
- `date_from`
- `date_to`
- `manager`

Unsupported DSL fields are now logged explicitly as:
`unsupported dsl filter field: ...`
(ignored for execution, not silently hidden).

## Р‘РѕРµРІРѕР№ Р·Р°РїСѓСЃРє Р±РµР· UI

РњРёРЅРёРјР°Р»СЊРЅС‹Р№ РѕРїРµСЂР°С†РёРѕРЅРЅС‹Р№ Р·Р°РїСѓСЃРє С‚РµРїРµСЂСЊ РґРµР»Р°РµС‚СЃСЏ С‡РµСЂРµР· PowerShell launcher:

1. РћС‚РєСЂС‹С‚СЊ С‚РµСЂРјРёРЅР°Р» РІ РєРѕСЂРЅРµ РїСЂРѕРµРєС‚Р°:
   - `D:\AI_Automation\amocrm_bot\project`
2. РђРєС‚РёРІРёСЂРѕРІР°С‚СЊ venv (РїСЂРёРјРµСЂ РґР»СЏ Windows PowerShell):
   - `.\.venv\Scripts\Activate.ps1`
3. Р—Р°РїСѓСЃС‚РёС‚СЊ launcher:
   - `.\scripts\run_reports.ps1`
4. Р’С‹Р±СЂР°С‚СЊ РїСѓРЅРєС‚ РјРµРЅСЋ:
   - `1` Analytics dry-run batch from sheet DSL
   - `2` Analytics live write block A1
   - `3` Analytics live write block F1
   - `4` Weekly refusals dry-run 2m
   - `5` Weekly refusals live 2m
   - `6` Weekly refusals live cumulative long

Launcher РїРµСЂРµРґ РєР°Р¶РґС‹Рј Р·Р°РїСѓСЃРєРѕРј РІС‹СЃС‚Р°РІР»СЏРµС‚:
- `GOOGLE_API_AUTH_MODE=cache_only`

Р­С‚Рѕ РёСЃРєР»СЋС‡Р°РµС‚ РЅРµРѕР¶РёРґР°РЅРЅС‹Р№ РёРЅС‚РµСЂР°РєС‚РёРІРЅС‹Р№ OAuth popup РІ РѕР±С‹С‡РЅРѕРј runtime.

### Dry-run vs Live write

- `dry-run`: discovery/compute/debug artifacts Р±РµР· С„Р°РєС‚РёС‡РµСЃРєРѕР№ Р·Р°РїРёСЃРё Р·РЅР°С‡РµРЅРёР№ РІ С‚Р°Р±Р»РёС†Сѓ.
- `live write`: С„Р°РєС‚РёС‡РµСЃРєРѕРµ РѕР±РЅРѕРІР»РµРЅРёРµ С†РµР»РµРІС‹С… Р±Р»РѕРєРѕРІ РІ Google Sheets.

РћРїРµСЂР°С†РёРѕРЅРЅС‹Р№ РїРѕСЂСЏРґРѕРє:
1. РЎРЅР°С‡Р°Р»Р° РІСЃРµРіРґР° РіРѕРЅСЏРµРј РЅР° С‚РµСЃС‚РѕРІС‹Р№ Р»РёСЃС‚.
2. РџСЂРѕРІРµСЂСЏРµРј debug/compiled artifacts.
3. РўРѕР»СЊРєРѕ РїРѕС‚РѕРј Р·Р°РїСѓСЃРєР°РµРј live write.

РџСѓС‚Рё Р°СЂС‚РµС„Р°РєС‚РѕРІ:
- debug: `D:\AI_Automation\amocrm_bot\project\exports\debug`
- compiled: `D:\AI_Automation\amocrm_bot\project\exports\compiled`

## amoCRM API Bootstrap
Minimal external integration OAuth bootstrap is documented in [docs/amocrm_auth_bootstrap.md](docs/amocrm_auth_bootstrap.md).


## Update (2026-04-18): Deal Analyzer Enrichment MVP (Read-only)

Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ РЅР° СЌС‚РѕРј СЌС‚Р°РїРµ:
- СЃСѓС‰РµСЃС‚РІСѓСЋС‰РёРµ analytics / weekly_refusals / Google Sheets writer flows РЅРµ РјРµРЅСЏР»РёСЃСЊ;
- РІ `deal_analyzer` РґРѕР±Р°РІР»РµРЅ read-only enrich pipeline РґР»СЏ РІРЅРµС€РЅРёС… С‚Р°Р±Р»РёС† Рё KPI-РєРѕРЅС‚РµРєСЃС‚Р°;
- Р·Р°РїРёСЃСЊ РѕР±СЂР°С‚РЅРѕ РІ Google Sheets РёР· enrich pipeline РЅРµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.

РќРѕРІС‹Рµ CLI-РєРѕРјР°РЅРґС‹:
- `python -m src.deal_analyzer.cli enrich-deal --config ... --input ...`
- `python -m src.deal_analyzer.cli enrich-period --config ... --input ...`
- `python -m src.deal_analyzer.cli roks-snapshot --config ... --manager "РР»СЊСЏ"`
- `python -m src.deal_analyzer.cli roks-snapshot --config ... --team`

РљР»СЋС‡РµРІС‹Рµ output-РїРѕР»СЏ РїРѕ СЃРґРµР»РєРµ:
- `enrichment_match_status`
- `enrichment_match_source`
- `enrichment_confidence`
- `matched_client_row_ref`
- `matched_appointment_row_ref`
- `manager_summary`
- `employee_coaching`
- `employee_fix_tasks`

Р”РµС‚Р°Р»Рё РєРѕРЅС‚СЂР°РєС‚Р° Рё РѕРіСЂР°РЅРёС‡РµРЅРёР№ СЃРј. РІ:
- `docs/deal_analyzer_enrichment_mvp.md`

## Update (2026-04-18): Deal Analyzer Call Evidence + Transcription MVP

- Added read-only call evidence layer (API-first, raw fallback, normalized fallback).
- Added transcription adapter layer with cache (`disabled/mock/local_placeholder/cloud_placeholder`).
- Added operator CLI commands: `collect-calls`, `transcribe-deal`, `transcribe-period`, `build-call-snapshot`.
- No changes in analytics / weekly_refusals / Google Sheets writer flows.
- Details: `docs/deal_analyzer_calls_mvp.md`.

## Update (2026-04-18): Storage Janitor MVP

???????? ?????????? janitor ???? ??? workspace/logs/caches (dry-run + apply) ? allowlist ? retention policy.

CLI:
- `python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json janitor-report`
- `python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json janitor-clean --dry-run`
- `python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json janitor-clean --apply`

??????: `docs/storage_janitor_mvp.md`.

## Update 2026-04-18: Test Hygiene + Janitor Targets

- Full suite command `python -m pytest -q -p no:cacheprovider tests` now passes without import mismatch.
- Root cause: duplicated test module names (`test_config.py`, `test_client.py`, `test_exporters.py`) across subfolders.
- Fix: test subfolders are package-marked (`__init__.py`) and guarded by a collection hygiene test.
- Janitor policy now includes:
  - `workspace/screenshots`
  - `workspace/tmp`
  - `workspace/tmp_tests`
  - `pytest-tmp`
  - `pytest_tmp_env`
- Safe default remains unchanged: janitor is disabled unless explicitly enabled (`janitor_enabled=true`).
- Default retention knobs:
  - `retention_days_screenshots` (default 14)
  - `keep_last_screenshots` (default 200)
  - `retention_days_tmp_dirs` (default 3)

## Update 2026-04-26: Daily Control LLM-First Path

Daily control active path uses a dedicated package:
- `src/deal_analyzer/daily_control/cli.py` (discover/build/write orchestration)
- `source_reader.py`, `day_grouper.py`, `roks_oap_resolver.py`, `roks_oap_parser.py`
- `daily_analyzer.py` (LLM-first manager-day analytics)
- `idempotency.py`, `writer_plan.py`, `sheets_writer.py`
- `validation/*` and `style/*` as separate technical layers.

Design contract:
- code builds facts/context and writes safely,
- LLM generates management narrative fields,
- scripted deterministic analytics phrases are not generated by code.

Language policy:
- blockers: foreign greeting/chinese/markdown fence/long foreign text in user-facing fields,
- warnings: allowed business Latin terms and technical terms,
- allowlist includes: `LINK`, `INFO`, `PLM`, `CRM`, `amoCRM`, `ID`, `URL`, `http`, `https`, `API`, `JSON`, `LLM`, `STT`, `ROKS`, `OAP`,
- row-level language repair runs before writer preflight,
- unrepaired rows are quarantined (row-level), not treated as whole-batch blockers by default.

Daily idempotency/update policy:
- base key: `period_start|period_end|control_day_date|manager_name`,
- exact key: base key + `sample_size|deals_count|calls_count`,
- exact match or same counts: skip,
- same base + bigger counts: update existing row,
- same base + smaller counts: stale skip,
- weird mismatch: conflict for review.

ROKS OAP month selection for period ending `2026-04-24`:
- current month: `???? ???-?????? 2026`,
- previous month: `???? ???-???? 2026`.

Main commands:
- discover:
  - `python -m src.deal_analyzer.daily_control.cli discover --config <config> --workbook "???? 2026" --daily-sheet "??????? ????????"`
- build dry-run:
  - `python -m src.deal_analyzer.daily_control.cli build --config <config> --period-start YYYY-MM-DD --period-end YYYY-MM-DD --source-sheet "?????? ???????" --daily-sheet "??????? ????????" --dry-run`
- write dry-run:
  - `python -m src.deal_analyzer.daily_control.cli write --config <config> --run-dir <run_dir> --daily-sheet "??????? ????????" --dry-run --strict-preflight --allow-partial-write --quarantine-unrepaired`

Real write must be run only as an explicit separate command after dry-run artifact review.

## Update 2026-04-27: Production Model Policy

Current production contour:
- `Р Р°Р·Р±РѕСЂ Р·РІРѕРЅРєРѕРІ`: DeepSeek (`qwen3.5:397b-cloud`) for analyze-period real-write (fallback `deepseek-v3.1:671b-cloud`).
- `Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ`: DeepSeek (`qwen3.5:397b-cloud`) with fallback (`deepseek-v3.1:671b-cloud`).
- `call_review_llm_replay` with Gemma: experimental only.
- `Р Р°Р·Р±РѕСЂ Р·РІРѕРЅРєРѕРІ` Р±РµСЂРµС‚ production РјРѕРґРµР»СЊ РёР· `config/deal_analyzer.call_review.deepseek.realwrite.json`.
- `Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ` Рё weekly-РјРѕРґСѓР»Рё РјРѕР¶РЅРѕ РїРµСЂРµРѕРїСЂРµРґРµР»СЏС‚СЊ С‡РµСЂРµР· CLI: `--main-model` / `--fallback-model`.

Safety for replay write:
- `call_review_llm_replay` always logs warning:
  - `EXPERIMENTAL: not recommended for production call review write`
- if `--write` is used with `--main-model gemma*`, replay requires:
  - `--allow-experimental-gemma-write`
- otherwise write is blocked with:
  - `experimental_gemma_write_requires_explicit_allow_flag`

Production command (`Р Р°Р·Р±РѕСЂ Р·РІРѕРЅРєРѕРІ`, DeepSeek):
```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.call_review.deepseek.realwrite.json analyze-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --period-mode current_week_to_date --discussion-limit 10 --limit 10
```

Daily control commands:
```powershell
python -m src.deal_analyzer.daily_control.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-03-30 --period-end 2026-04-24 --source-sheet "Р Р°Р·Р±РѕСЂ Р·РІРѕРЅРєРѕРІ" --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
python -m src.deal_analyzer.daily_control.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <daily_run_dir> --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --dry-run --strict-preflight
```

Experimental Gemma replay (dry-run only):
```powershell
python -m src.deal_analyzer.call_review_llm_replay --run-dir workspace/deal_analyzer/period_runs/20260425_224156 --config config/deal_analyzer.call_review.deepseek.realwrite.json --main-model gemma4:31b-cloud --fallback-model gpt-oss:20b --fallback2-model deepseek-v3.1:671b-cloud --limit 3 --dry-run --strict-preflight --allow-partial-write --quarantine-failed
```

## Weekly cycle (dry-run only)

Week plan discovery:
```powershell
python -m src.deal_analyzer.week_plan.cli discover --config config/deal_analyzer.call_review.deepseek.realwrite.json --target-sheet "РџР»Р°РЅ РЅРµРґРµР»Рё"
```

Week plan build (signals from previous week, plan for target week):
```powershell
python -m src.deal_analyzer.week_plan.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --signal-start 2026-04-20 --signal-end 2026-04-26 --plan-week-start 2026-04-27 --plan-week-end 2026-05-03 --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --target-sheet "РџР»Р°РЅ РЅРµРґРµР»Рё" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Weekly manager summary build:
```powershell
python -m src.deal_analyzer.weekly_manager_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --plan-sheet "РџР»Р°РЅ РЅРµРґРµР»Рё" --target-sheet "РќРµРґРµР»СЊРЅС‹Р№ СЃРІРѕРґ РјРµРЅРµРґР¶РµСЂРѕРІ" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Week summary build:
```powershell
python -m src.deal_analyzer.week_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --plan-sheet "РџР»Р°РЅ РЅРµРґРµР»Рё" --manager-summary-sheet "РќРµРґРµР»СЊРЅС‹Р№ СЃРІРѕРґ РјРµРЅРµРґР¶РµСЂРѕРІ" --target-sheet "РЎРІРѕРґ РЅРµРґРµР»Рё" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Integrated weekly cycle (in-memory, no sheet writes):
```powershell
python -m src.deal_analyzer.weekly_shared.pipeline_cli build-cycle --config config/deal_analyzer.call_review.deepseek.realwrite.json --signal-start 2026-04-20 --signal-end 2026-04-26 --plan-week-start 2026-04-27 --plan-week-end 2026-05-03 --daily-sheet "Р”РЅРµРІРЅРѕР№ РєРѕРЅС‚СЂРѕР»СЊ" --plan-sheet "РџР»Р°РЅ РЅРµРґРµР»Рё" --manager-summary-sheet "РќРµРґРµР»СЊРЅС‹Р№ СЃРІРѕРґ РјРµРЅРµРґР¶РµСЂРѕРІ" --week-summary-sheet "РЎРІРѕРґ РЅРµРґРµР»Рё" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```


## ROKS interpretation note

Manager-level ROKS funnel is role-based and may be non-linear.

- `demo > interest` for Ilya Bochkov is allowed when demos include routed meetings.
- Rustam Khomidov is evaluated primarily on top-of-funnel (`дозвоны/ЛПР/есть интерес`); downstream stages may be not applicable.
- Weekly texts must use role-correct wording (e.g. `провел N демо` vs `назначил N демо`).

Details: `docs/roks_interpretation.md`.
