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

- Stable call-review real-write profile uses `deepseek-v3.1:671b-cloud` as `ollama_model`.
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


Р­С‚РѕС‚ РїСЂРѕРµРєС‚ вЂ” Р±РµР·РѕРїР°СЃРЅС‹Р№ Р»РѕРєР°Р»СЊРЅС‹Р№ РєР°СЂРєР°СЃ РґР»СЏ РїРѕС€Р°РіРѕРІРѕР№ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРё РЅР° РґРѕРјР°С€РЅРµР№ Windows-РјР°С€РёРЅРµ.
РўРµРєСѓС‰РёР№ С€Р°Рі РґРѕР±Р°РІР»СЏРµС‚ read-only MVP Р±СЂР°СѓР·РµСЂРЅРѕРіРѕ С‡С‚РµРЅРёСЏ Р°РЅР°Р»РёС‚РёРєРё amoCRM: СЃРєСЂРёРїС‚ РѕС‚РєСЂС‹РІР°РµС‚ РёРЅС‚РµСЂС„РµР№СЃ, С‡РёС‚Р°РµС‚ С‚РµРєСѓС‰РёРµ С†РёС„СЂС‹ Рё СЃРѕС…СЂР°РЅСЏРµС‚ СЂРµР·СѓР»СЊС‚Р°С‚ РІ `exports`.

## Р­С‚Р°РїС‹ СЂРµР°Р»РёР·Р°С†РёРё

1. MVP Р·Р°РїРѕР»РЅРµРЅРёСЏ Р»РёСЃС‚Р° "РІРѕСЂРѕРЅРєР° РѕС‚РєР°Р·РѕРІ"
2. Weekly summary РїРѕ РѕС‚РєР°Р·Р°Рј
3. РђРЅР°Р»РёР· СЃРґРµР»РѕРє, Р·РІРѕРЅРєРѕРІ Рё РїСЂРµР·РµРЅС‚Р°С†РёР№

## Р§С‚Рѕ СѓР¶Рµ РµСЃС‚СЊ

- РР·РѕР»РёСЂРѕРІР°РЅРЅР°СЏ СЃС‚СЂСѓРєС‚СѓСЂР° РґРёСЂРµРєС‚РѕСЂРёР№ РІРЅСѓС‚СЂРё `project`
- Р‘Р°Р·РѕРІР°СЏ РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ С‡РµСЂРµР· `.env`
- РџСЂРѕРІРµСЂРєРё Р±РµР·РѕРїР°СЃРЅРѕСЃС‚Рё РїСѓС‚РµР№ (Р·Р°РїСЂРµС‚ РІС‹С…РѕРґР° Р·Р° РїСЂРµРґРµР»С‹ РїСЂРѕРµРєС‚Р°)
- Р›РѕРіРёСЂРѕРІР°РЅРёРµ РІ РєРѕРЅСЃРѕР»СЊ Рё С„Р°Р№Р»
- Browser read-only MVP РґР»СЏ amoCRM Р°РЅР°Р»РёС‚РёРєРё:
  - Playwright-СЃРµСЃСЃРёСЏ СЃ `storage state`
  - Р±РѕР»СЊС€РѕРµ РѕРєРЅРѕ Р±СЂР°СѓР·РµСЂР° РґР»СЏ СЃС‚Р°Р±РёР»СЊРЅРѕРіРѕ layout (`--start-maximized`, `no_viewport=True`)
  - С‡С‚РµРЅРёРµ С‚РµРєСѓС‰РµРіРѕ СЌРєСЂР°РЅР° Р°РЅР°Р»РёС‚РёРєРё
  - DOM-debug РґР°РјРїС‹ РґР»СЏ РїРѕРґР±РѕСЂР° СЃРµР»РµРєС‚РѕСЂРѕРІ
  - СЃРєСЂРёРЅС€РѕС‚ + СЌРєСЃРїРѕСЂС‚ JSON/CSV РІ `exports`
- РџРѕРґРіРѕС‚РѕРІРёС‚РµР»СЊРЅС‹Р№ config-driven СЃР»РѕР№:
  - `config/page_profiles.yaml`
  - `config/report_profiles.yaml`
  - `config/table_mappings.yaml`
  - `src/config_loader.py`
- РџРѕСЃС‚РѕСЏРЅРЅС‹Рµ РїСЂР°РІРёР»Р° Р°РіРµРЅС‚РЅРѕР№ СЂР°Р·СЂР°Р±РѕС‚РєРё РІ `AGENTS.md`

## РЈСЃС‚Р°РЅРѕРІРєР°

1. РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Python 3.11+.
2. РЎРѕР·РґР°С‚СЊ Рё Р°РєС‚РёРІРёСЂРѕРІР°С‚СЊ РІРёСЂС‚СѓР°Р»СЊРЅРѕРµ РѕРєСЂСѓР¶РµРЅРёРµ.
3. РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Р·Р°РІРёСЃРёРјРѕСЃС‚Рё:
   `pip install -r requirements.txt`
4. РЈСЃС‚Р°РЅРѕРІРёС‚СЊ Р±СЂР°СѓР·РµСЂ РґР»СЏ Playwright:
   `python -m playwright install chromium`
5. РЎРєРѕРїРёСЂРѕРІР°С‚СЊ `.env.example` РІ `.env` Рё Р·Р°РїРѕР»РЅРёС‚СЊ Р·РЅР°С‡РµРЅРёСЏ, РѕСЃРѕР±РµРЅРЅРѕ:
   - `AMO_BASE_URL`
   - `AMO_ANALYTICS_URL`
   - `AMO_VIEWPORT_WIDTH` / `AMO_VIEWPORT_HEIGHT` (РґР»СЏ headless СЂРµР¶РёРјР°)

## РџРµСЂРІС‹Р№ СЂСѓС‡РЅРѕР№ Р·Р°РїСѓСЃРє

1. Р’ `.env` РїРѕСЃС‚Р°РІРёС‚СЊ `AMO_HEADLESS=false`.
2. Р—Р°РїСѓСЃС‚РёС‚СЊ reader СЃ СЂСѓС‡РЅРѕР№ РїР°СѓР·РѕР№:
   `python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --wait-for-enter`
3. Р’ РѕРєРЅРµ Р±СЂР°СѓР·РµСЂР° РїСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё РІРѕР№С‚Рё РІ amoCRM.
4. РћС‚РєСЂС‹С‚СЊ РЅСѓР¶РЅС‹Р№ СЌРєСЂР°РЅ Р°РЅР°Р»РёС‚РёРєРё.
5. Р’С‹СЃС‚Р°РІРёС‚СЊ С„РёР»СЊС‚СЂС‹ Рё РІРєР»Р°РґРєСѓ РІСЂСѓС‡РЅСѓСЋ.
6. Р’РµСЂРЅСѓС‚СЊСЃСЏ РІ С‚РµСЂРјРёРЅР°Р» Рё РЅР°Р¶Р°С‚СЊ Enter.
7. Reader СЃС‡РёС‚Р°РµС‚ С‚РµРєСѓС‰РёР№ СЌРєСЂР°РЅ Рё СЃРѕС…СЂР°РЅРёС‚ screenshot + JSON/CSV.

## Р СѓС‡РЅРѕР№ Р»РѕРіРёРЅ Рё СЂСѓС‡РЅР°СЏ РїРѕРґРіРѕС‚РѕРІРєР° СЌРєСЂР°РЅР°

Р•СЃР»Рё РЅРµ С…РѕС‚РёС‚Рµ, С‡С‚РѕР±С‹ СЃРєСЂРёРїС‚ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РѕС‚РєСЂС‹РІР°Р» `AMO_ANALYTICS_URL`, РёСЃРїРѕР»СЊР·СѓР№С‚Рµ `--skip-open`:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --tab-mode all --skip-open --wait-for-enter`

## Р РµРєРѕРјРµРЅРґСѓРµРјС‹Р№ РїСЂР°РєС‚РёС‡РµСЃРєРёР№ СЂРµР¶РёРј (manual all-tab-modes)

Р”Р»СЏ Р±Р»РёР¶Р°Р№С€РµР№ СЃС‚Р°Р±РёР»СЊРЅРѕР№ СЂР°Р±РѕС‚С‹ РёСЃРїРѕР»СЊР·СѓР№С‚Рµ РїРѕР»СѓР°РІС‚РѕРјР°С‚РёС‡РµСЃРєРёР№ СЂРµР¶РёРј:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes-manual`

РљР°Рє СЌС‚Рѕ СЂР°Р±РѕС‚Р°РµС‚:

- РїРѕСЃР»Рµ РїРµСЂРІРѕРіРѕ Enter reader С‡РёС‚Р°РµС‚ С‚РµРєСѓС‰РёР№ СЌРєСЂР°РЅ РєР°Рє `all` Рё СЃСЂР°Р·Сѓ СЃРѕС…СЂР°РЅСЏРµС‚ export;
- Р·Р°С‚РµРј РїСЂРѕСЃРёС‚ РІСЂСѓС‡РЅСѓСЋ РїРµСЂРµРєР»СЋС‡РёС‚СЊ РІРєР»Р°РґРєСѓ РЅР° `РђРљРўРР’РќР«Р•` Рё РЅР°Р¶Р°С‚СЊ Enter;
- С‡РёС‚Р°РµС‚ `active` Рё СЃСЂР°Р·Сѓ СЃРѕС…СЂР°РЅСЏРµС‚ export;
- Р·Р°С‚РµРј РїСЂРѕСЃРёС‚ РІСЂСѓС‡РЅСѓСЋ РїРµСЂРµРєР»СЋС‡РёС‚СЊ РІРєР»Р°РґРєСѓ РЅР° `Р—РђРљР Р«РўР«Р•` Рё РЅР°Р¶Р°С‚СЊ Enter;
- С‡РёС‚Р°РµС‚ `closed` Рё СЃСЂР°Р·Сѓ СЃРѕС…СЂР°РЅСЏРµС‚ export.

Р’ СЌС‚РѕРј СЂРµР¶РёРјРµ РЅРµС‚ Р°РІС‚РѕРєР»РёРєРѕРІ РїРѕ РІРєР»Р°РґРєР°Рј, РїРѕСЌС‚РѕРјСѓ РѕРЅ РЅР°РґРµР¶РЅРµРµ РєР°Рє workaround, РїРѕРєР° auto-switching РµС‰Рµ РґРѕСЂР°Р±Р°С‚С‹РІР°РµС‚СЃСЏ.

## Profile-driven analytics flow (РЅРѕРІС‹Р№ С€Р°Рі)

Р”РѕР±Р°РІР»РµРЅ РїРµСЂРІС‹Р№ profile-driven СЂРµР¶РёРј:

`python -m src.run_profile_analytics --report-id analytics_tag_single_example`

Р§С‚Рѕ РґРµР»Р°РµС‚ СЂРµР¶РёРј:

- Р·Р°РіСЂСѓР¶Р°РµС‚ report profile РёР· `config/report_profiles.yaml`;
- РѕС‚РєСЂС‹РІР°РµС‚ `analytics_sales` СЌРєСЂР°РЅ;
- РїС‹С‚Р°РµС‚СЃСЏ РѕС‚РєСЂС‹С‚СЊ С„РёР»СЊС‚СЂ Рё РІС‹СЃС‚Р°РІРёС‚СЊ `filter_source` (`tag` РёР»Рё `utm_source`) + `filter_values`;
- РЅР°Р¶РёРјР°РµС‚ `РџСЂРёРјРµРЅРёС‚СЊ`;
- Р·Р°РїСѓСЃРєР°РµС‚ capture РІРєР»Р°РґРѕРє РїРѕ URL `deals_type=all/active/closed`;
- СЃРѕС…СЂР°РЅСЏРµС‚ JSON/CSV РїРѕ РєР°Р¶РґРѕР№ РІРєР»Р°РґРєРµ.

РќР° СЌС‚РѕРј СЌС‚Р°РїРµ СЌС‚Рѕ РїРµСЂРІС‹Р№ С€Р°Рі Рє РїРѕР»РЅРѕРјСѓ automation flow: `profile -> filter -> all/active/closed capture`.
Р•СЃР»Рё automation С„РёР»СЊС‚СЂР° РЅРµ СЃСЂР°Р±РѕС‚Р°Р» РёР·-Р·Р° СЃРµР»РµРєС‚РѕСЂРѕРІ, СЃРјРѕС‚СЂРёС‚Рµ debug screenshots РІ `workspace/screenshots` Рё debug dumps РїР°РЅРµР»Рё С„РёР»СЊС‚СЂР° РІ `exports/debug/` (`*_filter_panel_visible_text_*.txt`, `*_filter_panel_selectors_*.json`).

Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅРѕ РІРєР»СЋС‡РµРЅ scroll-debug РїР°РЅРµР»Рё С„РёР»СЊС‚СЂР°: СЃРѕР·РґР°СЋС‚СЃСЏ РїРѕС€Р°РіРѕРІС‹Рµ С„Р°Р№Р»С‹ `*_filter_panel_scroll_step_XX.txt` Рё РѕР±СЉРµРґРёРЅРµРЅРЅС‹Р№ `*_filter_panel_scroll_merged.txt`, С‡С‚РѕР±С‹ СѓРІРёРґРµС‚СЊ РїРѕР»РЅС‹Р№ СЃРїРёСЃРѕРє С„РёР»СЊС‚СЂРѕРІ РїРѕСЃР»Рµ РїСЂРѕРєСЂСѓС‚РєРё.
## Compile РІРµСЂС…РЅРµРіРѕ Р±Р»РѕРєР° (РїРµСЂРІС‹Р№ writer С€Р°Рі)

РџРѕСЃР»Рµ СЃР±РѕСЂР° С‚СЂРµС… JSON (`all/active/closed`) РјРѕР¶РЅРѕ СЃРѕР±СЂР°С‚СЊ РіРѕС‚РѕРІС‹Р№ compiled CSV РґР»СЏ РІРµСЂС…РЅРµРіРѕ Р±Р»РѕРєР°:

`python -m src.run_compile_top_block`

Р§С‚Рѕ РґРµР»Р°РµС‚ СЌС‚РѕС‚ С€Р°Рі:

- С‡РёС‚Р°РµС‚ snapshot JSON РґР»СЏ `all`, `active`, `closed` (Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё Р±РµСЂРµС‚ РїРѕСЃР»РµРґРЅРёРµ РёР· `exports/`);
- РёСЃРїРѕР»СЊР·СѓРµС‚ `top_cards` РєР°Рє РѕСЃРЅРѕРІРЅРѕР№ РёСЃС‚РѕС‡РЅРёРє;
- С„РѕСЂРјРёСЂСѓРµС‚ РїР»РѕСЃРєРёР№ CSV РІ `exports/compiled/`:
  - `stage_name`
  - `all_count`
  - `active_count`
  - `closed_count`
- РµСЃР»Рё СЌС‚Р°Рї РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ РѕРґРЅРѕР№ РёР· РІРєР»Р°РґРѕРє, СЃС‚Р°РІРёС‚ `0`.

Р­С‚Рѕ РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅС‹Р№ РїСЂР°РєС‚РёС‡РµСЃРєРёР№ С€Р°Рі РїРµСЂРµРґ Р·Р°РїРёСЃСЊСЋ РІ СЂРµР°Р»СЊРЅСѓСЋ С‚Р°Р±Р»РёС†Сѓ (Google Sheets write РїРѕРєР° РЅРµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ).

## РђРІС‚Рѕ-РїСЂРѕРіРѕРЅ РІСЃРµС… РІРєР»Р°РґРѕРє (URL-based)

РњРѕР¶РЅРѕ РїРѕРґРіРѕС‚РѕРІРёС‚СЊ СЌРєСЂР°РЅ РѕРґРёРЅ СЂР°Р· РІСЂСѓС‡РЅСѓСЋ Рё Р·Р°РїСѓСЃС‚РёС‚СЊ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРѕРµ РїРµСЂРµРєР»СЋС‡РµРЅРёРµ РІРєР»Р°РґРѕРє:

`python -m src.run_read_analytics --source-kind tag --filter-id manual --skip-open --wait-for-enter --all-tab-modes`

Р’ СЌС‚РѕРј СЂРµР¶РёРјРµ reader РїРµСЂРµРєР»СЋС‡Р°РµС‚ РІРєР»Р°РґРєРё С‡РµСЂРµР· URL-РїР°СЂР°РјРµС‚СЂ `deals_type`, Р±РµР· UI-РєР»РёРєРѕРІ РїРѕ РІРєР»Р°РґРєР°Рј:

- `deals_type=all`
- `deals_type=active`
- `deals_type=closed`

РљР°Р¶РґР°СЏ СѓСЃРїРµС€РЅРѕ РїСЂРѕС‡РёС‚Р°РЅРЅР°СЏ РІРєР»Р°РґРєР° СЌРєСЃРїРѕСЂС‚РёСЂСѓРµС‚СЃСЏ СЃСЂР°Р·Сѓ (JSON + CSV).
Р•СЃР»Рё С‡С‚РµРЅРёРµ СЃР»РµРґСѓСЋС‰РµР№ РІРєР»Р°РґРєРё РЅРµ СѓРґР°Р»РѕСЃСЊ, СѓР¶Рµ СЃРѕС…СЂР°РЅРµРЅРЅС‹Рµ С„Р°Р№Р»С‹ РѕСЃС‚Р°СЋС‚СЃСЏ РІ `exports/` Рё РЅРµ С‚РµСЂСЏСЋС‚СЃСЏ.

`--all-tab-modes-manual` РѕСЃС‚Р°РµС‚СЃСЏ Р·Р°РїР°СЃРЅС‹Рј СЂРµР¶РёРјРѕРј: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РІСЂСѓС‡РЅСѓСЋ РїРµСЂРµРєР»СЋС‡Р°РµС‚ РІРєР»Р°РґРєРё Рё РїРѕРґС‚РІРµСЂР¶РґР°РµС‚ С€Р°РіРё Enter.

## Р Р°СЃС€РёСЂСЏРµРјР°СЏ РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ

РРґРµСЏ РїСЂРѕСЃС‚Р°СЏ:

- РєРѕРґ = РґРІРёР¶РѕРє С‡С‚РµРЅРёСЏ/РѕР±СЂР°Р±РѕС‚РєРё;
- config = С‡С‚Рѕ РёРјРµРЅРЅРѕ Р·Р°РїСѓСЃРєР°С‚СЊ Рё РєСѓРґР° СЃРєР»Р°РґС‹РІР°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚.

Р”Р»СЏ СЂСѓС‡РЅРѕРіРѕ СЂРµРґР°РєС‚РёСЂРѕРІР°РЅРёСЏ РёСЃРїРѕР»СЊР·СѓСЋС‚СЃСЏ YAML-С„Р°Р№Р»С‹ РІ `config/`:

- `page_profiles.yaml` вЂ” РєР°РєРёРµ С‚РёРїС‹ СЃС‚СЂР°РЅРёС† amoCRM РµСЃС‚СЊ РІ РїСЂРѕРµРєС‚Рµ;
- `report_profiles.yaml` вЂ” РєР°РєРёРµ РѕС‚С‡РµС‚С‹ Р·Р°РїСѓСЃРєР°С‚СЊ, СЃ РєР°РєРёРјРё С„РёР»СЊС‚СЂР°РјРё/РІРєР»Р°РґРєР°РјРё/РёСЃС‚РѕС‡РЅРёРєР°РјРё;
- `table_mappings.yaml` вЂ” РєСѓРґР° РїРёСЃР°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚ (С†РµР»РµРІС‹Рµ Р±Р»РѕРєРё/СЂРµР¶РёРјС‹ Р·Р°РїРёСЃРё) РЅР° СЃР»РµРґСѓСЋС‰РёС… СЌС‚Р°РїР°С….

Р’ Р±СѓРґСѓС‰РµРј РЅРѕРІС‹Рµ С‚РµРіРё, РЅРѕРІС‹Рµ РѕС‚С‡РµС‚С‹, РЅРѕРІС‹Рµ СЃС‚СЂР°РЅРёС†С‹ amoCRM (`analytics`, `deals`, `events`) РјРѕР¶РЅРѕ Р±СѓРґРµС‚ РґРѕР±Р°РІР»СЏС‚СЊ С‡РµСЂРµР· config Р±РµР· РїРµСЂРµРїРёСЃС‹РІР°РЅРёСЏ СЏРґСЂР°.

## Р’Р°Р¶РЅРѕ РїСЂРѕ РѕРіСЂР°РЅРёС‡РµРЅРёСЏ MVP

- Read-only РїРѕРІРµРґРµРЅРёРµ: РЅРёРєР°РєРёС… РґРµР№СЃС‚РІРёР№ `save/submit/delete`.
- РќР° СЌС‚РѕРј С€Р°РіРµ С„РёР»СЊС‚СЂС‹ РІС‹СЃС‚Р°РІР»СЏСЋС‚СЃСЏ РІСЂСѓС‡РЅСѓСЋ РІ amoCRM UI.







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

- amoCRM field `Типы событий` is rendered as `checkboxes-search` (not standard select/dropdown).
- Valid scope can be the control root itself (`filter__custom_settings__item checkboxes-search js-control-checkboxes-search`).
- Primary search-kind selectors:
  - open/check state: `.checkboxes-search__opening-list`, `.checkboxes-search__search-input`, `.checkboxes-search__section-common`, `.checkboxes-search__item-label`, `input[type='checkbox'][data-value]`
  - option resolve: `.checkboxes-search__item-label:has-text(...)`, `label:has(input[data-value='...'])`, `input[type='checkbox'][data-value='...']`
  - apply: `.js-checkboxes-search-list-apply` (including `.checkboxes-search__buttons-wrapper .button-input`) and `OK/ОК` variants.
- Do not use page-wide `label/li/input[type='checkbox']` for this stage: it can click left preset panel instead of opened `Типы событий` widget.
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

## Боевой запуск без UI

Минимальный операционный запуск теперь делается через PowerShell launcher:

1. Открыть терминал в корне проекта:
   - `D:\AI_Automation\amocrm_bot\project`
2. Активировать venv (пример для Windows PowerShell):
   - `.\.venv\Scripts\Activate.ps1`
3. Запустить launcher:
   - `.\scripts\run_reports.ps1`
4. Выбрать пункт меню:
   - `1` Analytics dry-run batch from sheet DSL
   - `2` Analytics live write block A1
   - `3` Analytics live write block F1
   - `4` Weekly refusals dry-run 2m
   - `5` Weekly refusals live 2m
   - `6` Weekly refusals live cumulative long

Launcher перед каждым запуском выставляет:
- `GOOGLE_API_AUTH_MODE=cache_only`

Это исключает неожиданный интерактивный OAuth popup в обычном runtime.

### Dry-run vs Live write

- `dry-run`: discovery/compute/debug artifacts без фактической записи значений в таблицу.
- `live write`: фактическое обновление целевых блоков в Google Sheets.

Операционный порядок:
1. Сначала всегда гоняем на тестовый лист.
2. Проверяем debug/compiled artifacts.
3. Только потом запускаем live write.

Пути артефактов:
- debug: `D:\AI_Automation\amocrm_bot\project\exports\debug`
- compiled: `D:\AI_Automation\amocrm_bot\project\exports\compiled`

## amoCRM API Bootstrap
Minimal external integration OAuth bootstrap is documented in [docs/amocrm_auth_bootstrap.md](docs/amocrm_auth_bootstrap.md).


## Update (2026-04-18): Deal Analyzer Enrichment MVP (Read-only)

Фактическое состояние на этом этапе:
- существующие analytics / weekly_refusals / Google Sheets writer flows не менялись;
- в `deal_analyzer` добавлен read-only enrich pipeline для внешних таблиц и KPI-контекста;
- запись обратно в Google Sheets из enrich pipeline не выполняется.

Новые CLI-команды:
- `python -m src.deal_analyzer.cli enrich-deal --config ... --input ...`
- `python -m src.deal_analyzer.cli enrich-period --config ... --input ...`
- `python -m src.deal_analyzer.cli roks-snapshot --config ... --manager "Илья"`
- `python -m src.deal_analyzer.cli roks-snapshot --config ... --team`

Ключевые output-поля по сделке:
- `enrichment_match_status`
- `enrichment_match_source`
- `enrichment_confidence`
- `matched_client_row_ref`
- `matched_appointment_row_ref`
- `manager_summary`
- `employee_coaching`
- `employee_fix_tasks`

Детали контракта и ограничений см. в:
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
