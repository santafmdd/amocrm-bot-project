
## Update (2026-04-10) ? Execution DSL Source Split

### What Changed
- `--execution-from-sheet-dsl` now resolves execution input from a dedicated DSL source target, not from writer destination.
- New optional CLI override: `--execution-source-target-id`.
- New report profile config section supported: `execution_input.target_id`.

### Routing Rule
- Execution DSL source: `execution_input.target_id` (or `--execution-source-target-id`).
- Writer output destination: `output.target_id`.
- These are now independent.

### Fallback
- If `execution_input.target_id` is missing, runtime falls back to writer target with explicit warning log.

### New Runtime Logs
- `execution_source=sheet_dsl`
- `execution_input_target_id=...`
- `execution_input_tab_name=...`
- `writer_target_id=...`
- `writer_tab_name=...`

### Error Handling
- If DSL source tab is missing, discovery now raises:
  `DSL discovery sheet tab not found: <tab>. Check execution_input.target_id/table_mappings.yaml`

# SESSION HANDOFF

## Update (2026-04-08)

### Service Goal (Confirmed)
Automation of amoCRM reporting routine by profiles:
`open screen -> set filter -> capture all/active/closed -> write to sheet`.

### MVP Priority (Current)
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

### Confirmed Working in Production Run
- `source_kind=tag`
- tag selection via holder popup path
- filter apply
- capture `all / active / closed`
- compiled artifacts: `compiled_profile` + `compiled_stage_pivot`
- Google Sheets API writer for first block
- run markers: `tag_selection_success=true`, `Filter apply confirmed`, `successful_tabs=3/3`, `updatedCells=30`, `fallback used=false`

### Not Production-Ready
- `utm_exact` flow
- `utm_prefix` flow
- `batch-from-sheet-dsl` production
- weekly refusals
- AI summary
- log retention / cleanup

### Next Step
Stabilize `utm_exact` analytics filter automation path (browser capture side only), then run block-2 write via existing API writer route.



## Status Update (2026-04-08)

### Service Goal
Automation of routine amoCRM analytics reporting by profiles:
1. Open analytics screen
2. Apply filters
3. Capture `all / active / closed`
4. Write results to Google Sheets

### Current MVP Priority
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block to sheet`.

### Confirmed Working
- `source_kind=tag`
- tag selection via holder-popup path
- filter apply
- capture `all / active / closed`
- compiled artifacts: `compiled_profile` + `compiled_stage_pivot`
- Google Sheets API writer for block 1

### Last Confirmed Production Run
- `tag_selection_success=true`
- `Filter apply confirmed`
- `successful_tabs=3/3`
- `updatedCells=30`
- `fallback used=false`

### Not Production-Ready Yet
- `utm_exact` flow
- `utm_prefix` flow
- `batch-from-sheet-dsl` production mode
- weekly refusals block
- AI summary
- cleanup / log retention policy

### Next Development Focus
Stabilize `utm_exact` filter automation in analytics browser flow and keep existing writer/discovery internals intact.


## Current Stage

Layout writer С‚РµРїРµСЂСЊ СѓРјРµРµС‚ РЅРµ С‚РѕР»СЊРєРѕ РїР°СЂСЃРёС‚СЊ DSL, РЅРѕ Рё Р·Р°РїСѓСЃРєР°С‚СЊ per-scenario execution РґР»СЏ `||` С‡РµСЂРµР· СЂРµР°Р»СЊРЅС‹Р№ UI amoCRM.

## What Was Added

1. New execution layer:
- `src/analytics/scenario_executor.py`
- РћСЃРЅРѕРІРЅР°СЏ С‚РѕС‡РєР°: `ScenarioExecutor.execute_block_scenarios(...)`

2. End-to-end scenario run per block:
- Р±РµСЂС‘Рј DSL Р¶С‘Р»С‚РѕР№ СЃС‚СЂРѕРєРё;
- СЂР°Р·Р±РёРІР°РµРј РЅР° `scenarios[]`;
- РґР»СЏ РєР°Р¶РґРѕРіРѕ СЃС†РµРЅР°СЂРёСЏ РѕС‚РґРµР»СЊРЅРѕ:
  - reset to clean analytics state;
  - open filter panel;
  - apply filters scenario;
  - apply and wait;
  - capture `all/active/closed` snapshots;
  - СЃС‡РёС‚Р°С‚СЊ score (`total_count`, `non_empty_stage_rows`).

3. Best scenario selection:
- max `total_count`
- tie -> max `non_empty_stage_rows`
- tie -> first successful

4. Layout writer integration:
- `src/writers/google_sheets_layout_ui_writer.py`
- РµСЃР»Рё Сѓ Р±Р»РѕРєР° РµСЃС‚СЊ DSL Рё РїРµСЂРµРґР°РЅ executor:
  - РІС‹РїРѕР»РЅСЏСЋС‚СЃСЏ СЂРµР°Р»СЊРЅС‹Рµ scenario runs;
  - Р±РµСЂС‘С‚СЃСЏ best scenario result;
  - СЃС‚СЂРѕРёС‚СЃСЏ block-specific pivot;
  - РІ С‚Р°Р±Р»РёС†Сѓ РїРёС€СѓС‚СЃСЏ Р·РЅР°С‡РµРЅРёСЏ С‚РѕР»СЊРєРѕ best scenario.

5. Entrypoint integration:
- `src/run_profile_analytics.py`
- РїСЂРё `destination.kind == google_sheets_layout_ui` СЃРѕР·РґР°С‘С‚СЃСЏ `ScenarioExecutor`
  Рё РїРµСЂРµРґР°С‘С‚СЃСЏ РІ layout writer.

6. Tests:
- `tests/test_scenario_executor.py` (selection/merge-level checks)

## Current Practical Limits

- Р¤РёР»СЊС‚СЂС‹ `pipeline/period/dates_mode` РїСЂРёРјРµРЅСЏСЋС‚СЃСЏ best-effort С‡РµСЂРµР· label-driven UI path.
- Р”Р»СЏ `^=` prefix РІ UI РїРѕРєР° С‡РµСЃС‚РЅС‹Р№ best-effort (СЃ warning, РµСЃР»Рё РїРѕР»Рµ/РјРµС…Р°РЅРёРєР° РЅРµ РЅР°Р№РґРµРЅС‹).
- РќРёР¶РЅСЏСЏ С‚Р°Р±Р»РёС†Р° РѕС‚РєР°Р·РѕРІ РїРѕ-РїСЂРµР¶РЅРµРјСѓ out-of-scope.

## Next Step

1. РЈСЃРёР»РёС‚СЊ field-specific apply РґР»СЏ `dates_mode/period/pipeline` (UI selectors per real account).
2. Р”РѕР±Р°РІРёС‚СЊ Р±РѕР»РµРµ СЃС‚СЂРѕРіСѓСЋ РІР°Р»РёРґР°С†РёСЋ РїСЂРёРјРµРЅС‘РЅРЅРѕРіРѕ С„РёР»СЊС‚СЂР° РїРµСЂРµРґ capture.
3. Р Р°СЃС€РёСЂРёС‚СЊ diagnostics per scenario (structured summary + screenshot map).


## New Debug Tool: Layout Grid Inspector

?? ? ?? ?? ? ? Google Sheets  layout reader.

:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-grid-inspector-only --browser-backend openclaw_cdp --tag-selection-mode script`

 :
-  `sheet_url` ? ? `tab_name`;
- ? grid ( ? amoCRM flow);
- ?? ?? ? ? ? ??  full discovery scan;
- ?? multi-strategy dump ? grid-like .

??   (`exports/debug`):
- `layout_grid_inspector_visible_<ts>.png`
- `layout_grid_inspector_elements_<ts>.json`
- `layout_grid_inspector_elements_<ts>.txt`
- `layout_grid_inspector_grid_snippet_<ts>.html`
- `layout_grid_inspector_top_text_<ts>.json`

 ?:
- ? selector counts > 0, ?? `inner_text/text_content` , ?? ?   ;
- ? counts == 0, ?? `grid_snippet` ? `top_text`  , ?? ? ? ;
- ? `ariaLabel/rowIndex/colIndex/dataRow/dataCol` ?  DSL-?? ??.


## Google Sheets API Verification (2026-04-03)

Status: confirmed working against real Google Sheets document.

Verified facts:
- Test command: `python .\test_google_sheets_api.py`
- Desktop/local OAuth flow completed in browser and callback page returned:
  - `The authentication flow has completed. You may close this window.`
- Read from real tab succeeded: `analytics_writer_test`
- Test write succeeded with API response:

```python
{
  'spreadsheetId': '1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0',
  'updatedRange': 'analytics_writer_test!J25',
  'updatedRows': 1,
  'updatedColumns': 1,
  'updatedCells': 1
}
```

Clarification:
- `desktop/local OAuth` means local auth flow + local token storage only.
- It does NOT mean a local spreadsheet copy.
- Read/write operations target the real Google-hosted sheet by `spreadsheetId`.
- If account has access in Google Sheets UI, the same account can read/write via API within granted scopes.

Architecture conclusion:
- Technical feasibility of moving from browser/UI-driven Sheets writer to API-driven writer is now confirmed.

Next roadmap step (priority):
1. Add dedicated Google Sheets API client/writer module.
2. Implement range/layout reading via API.
3. Implement layout/block anchor discovery via API (no browser grid scan as primary path).
4. Implement analytics writes into resolved ranges.
5. Keep browser/OpenClaw writer path as fallback/debug path.

Operational notes:
- OAuth tokens are stored locally.
- Credentials/token files must stay in `.gitignore`.
- API path should be separated from browser/OpenClaw path.
- Current test confirms baseline read/write capability.


## API Discovery Inspector (Read-Only)

Added isolated Google Sheets API discovery mode (no browser discovery, no amoCRM execution, no writes):

- CLI: `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-inspector-only`
- Uses destination from `table_mappings.yaml` and runs API-only layout discovery.

What it does:
- reads spreadsheet metadata and target tab via API;
- scans ranges in bounded bands (`A1:Z120`-style, configurable via layout `api_*` options);
- detects DSL rows and stage header rows;
- builds block anchors when header is found under DSL row.

Artifacts (`exports/debug`):
- `layout_api_discovery_reads_<ts>.json`
- `layout_api_discovery_summary_<ts>.txt`

Important:
- this step is read-only;
- production write path is unchanged;
- browser/OpenClaw writer path remains available as fallback/debug.

Next step:
- implement API write path into resolved block ranges.


## API Layout Write Path (Stage Update)

Added a new API-based layout write path (no browser cell writes):
- CLI flags:
  - `--writer-layout-api-write`
  - `--writer-layout-api-dry-run`

Behavior:
- uses API discovery anchors (`dsl_row/header_row/stage_col/all_col/active_col/closed_col`),
- reads stage rows under selected header,
- matches stage names from compiled analytics,
- writes `all/active/closed` values via `spreadsheets.values.batchUpdate`.

Safety:
- existing browser/OpenClaw writer path remains unchanged (default),
- API path is opt-in via flags,
- dry-run produces plan artifacts without writing.

- Debug screenshots in analytics flow are now best-effort: screenshot timeout/errors are logged as warnings and do not abort run_profile_analytics (including API write dry-run).


## Isolated API Block Targeting (from latest compiled)

Added isolated mode for API layout writer without amoCRM/browser flow:
- `--writer-layout-api-write-from-latest-compiled`

New optional target selectors:
- `--writer-layout-api-target-dsl-row <int>`
- `--writer-layout-api-target-dsl-text-contains "<text>"`

Behavior:
- loads latest `exports/compiled/compiled_stage_pivot_*.json` (and `compiled_profile_*.json` if available),
- runs API discovery + anchor selection + API layout write/dry-run,
- does not start Browser/OpenClaw and does not run `analytics_flow`.

Logs and summary now include:
- `target_selector`
- `selected_anchor`
- `next_anchor_dsl_row`
- `hard_row_upper_bound`
- `stage_rows_selected_count`
- `stop_reason`

Use cases:
- validate block #2/#3 boundaries and planned updates independently from unstable UI tag selection path.

## Production Routing Update (2026-04-04)

New preferred production path is now available for `google_sheets_layout_ui`:
1. browser/OpenClaw captures analytics and builds compiled result;
2. Google Sheets API layout writer performs write (or dry-run plan);
3. browser UI layout writer remains fallback/debug path.

New CLI flags:
- `--writer-layout-api-preferred`
- `--writer-layout-api-fallback-to-ui`

Routing logs now include:
- `compiled result built = true`
- `writer mode selected = api_preferred|api_opt_in`
- `api discovery start|finish`
- `api write success|fail`
- `fallback used = true|false`

Dry-run for full browser capture + API write plan:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`

## Batch DSL Routing (2026-04-05)

Added routing layer:
- `DSL block -> execution input -> per-block compiled result -> write into exact block`.

New CLI:
- `--writer-layout-api-batch-from-sheet-dsl-dry-run`
- `--writer-layout-api-batch-from-sheet-dsl`

Dry-run mode:
- discovers anchors,
- parses DSL per block,
- logs source mapping (`tag` / `utm_exact` / `utm_prefix`),
- does not run capture/write.

Production batch mode:
- executes scenarios per anchor DSL,
- saves compiled artifacts per block run,
- writes each block via API writer with `target_dsl_row`.

Existing low-level/manual modes remain unchanged:
- `--writer-layout-api-write-from-latest-compiled`
- `--writer-layout-api-target-dsl-row`
- `--writer-layout-api-target-dsl-text-contains`.

## Update (2026-04-09): Browser Filter Handlers v1

Refactor status (minimal-intrusive):
- `analytics_flow.py` now routes supported filter applications through `FilterRegistry` handlers.
- New handler interface is unified (`resolve/apply/verify/debug_dump`).
- Current supported browser-layer handlers (v1):
  - `tag` (holder popup path, kept as primary working path)
  - `utm_source` exact
  - `utm_source` prefix (best-effort UI path with warning)
  - `pipeline`
  - `date` (shared dates/period control)
  - `manager`

Scope note:
- Writer/discovery/DSL internals were not changed in this refactor step.
- `analytics_flow` remains backward compatible on public signatures.

Unsupported / not production-ready in this step:
- weekly refusals automation
- AI summary generation
- full production stability for `utm_exact/utm_prefix` scenario execution across all accounts
- universal filter-field mapping for every amoCRM custom UI variant

## Runtime Validation Status (2026-04-09)

Scope: supported filters v1 at browser-layer (`tag`, `utm_source exact`, `pipeline`, `date`, `manager`).

### Code-level Verified
- `tag` handler routed via `FilterRegistry` and covered by unit tests.
- `utm_source exact` handler routed via `FilterRegistry` and covered by unit tests.
- `pipeline` / `date` / `manager` handlers exist, routed via registry, and have smoke unit tests.

### Runtime Verified
- `tag`: verified in real runs (`tag_selection_success=true`, capture `all/active/closed`, apply confirmed).
- `utm_source exact`: runtime path exists via `analytics_utm_single_example`, but still considered pending full stable validation in latest handoff.

### Runtime Pending
- `pipeline`: no dedicated standalone runtime profile; currently validated only as additional DSL fields in batch flow.
- `date`: no dedicated standalone runtime profile; currently validated only as additional DSL fields in batch flow.
- `manager`: no runtime-ready profile and no current sheet DSL block confirmed with manager field.

### Current Runtime Assets Found
- `config/report_profiles.yaml` contains:
  - `analytics_tag_single_example`
  - `analytics_utm_single_example`
- Latest batch dry-run summary (`exports/debug/layout_api_batch_from_sheet_dsl_summary_20260409_110423.json`) confirms DSL blocks for:
  - tag + date + period + pipeline
  - utm exact + date + period + pipeline
  - utm prefix + date + period + pipeline
- No manager DSL block detected in current summary.

### Minimal Additions Needed for Full Runtime Validation
- Add one runtime-ready DSL block with manager, e.g.:
  - `...; Менеджер=<name>; Теги=<value>`
  or
  - `...; Менеджер=<name>; utm_source=<value>`
- Optional: add dedicated report profiles for manual single-scenario checks if needed.

## Runtime Status Update (2026-04-09)

### Current Runtime State
- `tag` flow: runtime verified (stable in recent re-checks), including URL-based post-apply verify.
- `utm_source exact`: still runtime-pending; code-level + unit tests are green, but final runtime stability must be confirmed by local OpenClaw runs.

### Golden Runtime Validation Commands
Tag (run 3 times):
1. `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`
2. repeat x2

UTM exact (run 2 times):
1. `python -m src.run_profile_analytics --report-id analytics_utm_single_example --writer-layout-api-preferred --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`
2. repeat x1

### Known Remaining Limits
- Normal Apply click may still be intercepted by overlay; JS apply fallback is still important.
- `all` tab readiness can be noisy on first attempt and may recover on retry.
- `utm_source exact` requires strict row/popup context; if popup is not opened for resolved row, flow now fails explicitly instead of broad fallback.

### UTM Exact Row-Scoped Diagnostics (How to Read)
Healthy row-scoped path typically shows:
- `utm_row_scope_resolved=true`
- `utm_row_multisuggest_id=<id or empty>`
- `active_popup_found=true`
- `utm_popup_multisuggest_id=<id>`
- `utm_popup_id_matches_row=true` (or empty-id tolerant case)
- `utm_input_multisuggest_id=<id>`
- `utm_input_id_matches_popup=true` (or empty-id tolerant case)
- `utm_exact_option_click_success=true` OR direct chip detection
- `utm_exact_selection_success=true`

Failure signals to inspect first:
- `utm_row_scope_resolved=false`
- `active_popup_found=false`
- `utm_exact_fail_reason=active_popup_not_opened`
- `utm_exact_fail_reason=row_scoped_input_not_activated`
- `utm_exact_fail_reason=chip_not_detected`
