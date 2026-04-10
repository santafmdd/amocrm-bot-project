# Analytics Layout Writer

## Update (2026-04-08)

### End Goal
`amoCRM capture by profile -> filter automation -> all/active/closed capture -> write to Google Sheets layout block`.

### MVP Focus
`profile-driven analytics flow -> filter automation -> capture all/active/closed -> write top block`.

### Confirmed Working
- `source_kind=tag` (holder popup path)
- filter apply and capture `all/active/closed`
- `compiled_profile` + `compiled_stage_pivot`
- API layout write for first block (`updatedCells=30`)
- successful markers: `tag_selection_success=true`, `Filter apply confirmed`, `successful_tabs=3/3`, `fallback used=false`

### Pending (Not Production)
- `utm_exact`
- `utm_prefix`
- `batch-from-sheet-dsl` production
- weekly refusals
- AI summary
- cleanup/log retention



## Runtime Snapshot (2026-04-08)

### Target Pipeline
`capture in amoCRM browser -> compiled artifacts -> Google Sheets API layout write`.

### MVP Scope Now
- profile-driven analytics capture
- filter automation
- tabs capture: `all / active / closed`
- API write for top stage block

### Confirmed Working
- `source_kind=tag` end-to-end
- holder-popup tag selection
- filter apply confirmation
- `successful_tabs=3/3`
- `compiled_profile_*` and `compiled_stage_pivot_*`
- API writer updates first block (`updatedCells=30`, no UI-writer fallback)

### Not Ready Yet
- `utm_exact` automation stability
- `utm_prefix` automation stability
- production batch from sheet DSL
- refusals table automation
- AI summary and retention/cleanup policies

### Immediate Next Step
Implement and validate dedicated `utm_source exact` browser filter path, then wire it to block #2 through the existing API writer route.


## Purpose

`google_sheets_layout_ui` вЂ” РѕСЃРЅРѕРІРЅРѕР№ writer РґР»СЏ stage-Р±Р»РѕРєРѕРІ РІ СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµРј С€Р°Р±Р»РѕРЅРµ Google Sheets.

РўРµРїРµСЂСЊ writer РґРµР»Р°РµС‚ СЂРµР°Р»СЊРЅС‹Р№ per-scenario execution (`||`) С‡РµСЂРµР· amoCRM UI РїРµСЂРµРґ Р·Р°РїРёСЃСЊСЋ Р±Р»РѕРєР°.

## Runtime Flow

1. РџРѕР»СѓС‡РёС‚СЊ compiled analytics (`all/active/closed`) РєР°Рє baseline.
2. РќР°Р№С‚Рё DSL-СЃС‚СЂРѕРєРё Рё stage-Р±Р»РѕРєРё РЅР° Р»РёСЃС‚Рµ.
3. Р”Р»СЏ Р±Р»РѕРєР° СЃ DSL:
   - РІС‹РїРѕР»РЅРёС‚СЊ РІСЃРµ scenario РёР· `||` С‡РµСЂРµР· `ScenarioExecutor`;
   - РєР°Р¶РґС‹Р№ scenario РїСЂРѕРіРѕРЅСЏРµС‚СЃСЏ РѕС‚РґРµР»СЊРЅРѕ (reset/apply/capture);
   - РІС‹Р±СЂР°С‚СЊ Р»СѓС‡С€РёР№ scenario РїРѕ score.
4. РџРѕСЃС‚СЂРѕРёС‚СЊ block-specific pivot РёР· best scenario.
5. Р—Р°РїРёСЃР°С‚СЊ С‚РѕР»СЊРєРѕ С‡РёСЃР»РѕРІС‹Рµ СЏС‡РµР№РєРё `Р’СЃРµ/РђРєС‚РёРІРЅС‹Рµ/Р—Р°РєСЂС‹С‚С‹Рµ`.

## Scenario Scoring

- Р±РѕР»СЊС€Рµ `total_count`
- Р·Р°С‚РµРј Р±РѕР»СЊС€Рµ `non_empty_stage_rows`
- Р·Р°С‚РµРј РїРµСЂРІС‹Р№ СѓСЃРїРµС€РЅС‹Р№

## Logs

РћР¶РёРґР°РµРјС‹Рµ РјР°СЂРєРµСЂС‹:
- `scenario execution start`
- `scenario filter apply success`
- `scenario result`
- `selected_best_scenario`
- `external per-scenario execution finished`
- `layout planned writes`

## Current Limits

- `dates_mode/period/pipeline` РїРѕРєР° best-effort UI apply;
- `^=` prefix Р·Р°РІРёСЃРёС‚ РѕС‚ СЂРµР°Р»СЊРЅРѕР№ РїРѕРґРґРµСЂР¶РєРё РїРѕР»СЏ UI (РёРЅР°С‡Рµ warning);
- РЅРёР¶РЅСЏСЏ С‚Р°Р±Р»РёС†Р° РѕС‚РєР°Р·РѕРІ РЅРµ РѕР±СЂР°Р±Р°С‚С‹РІР°РµС‚СЃСЏ.


## Canonical DSL examples (UI-exact field names)

- `??: ?=?; =??  ??; ?=?? (2 ); ?=?? || ?=?; =??  ??; ?=?? (2 ); UTM Source^=conf_novosib_mechanical_engineering_2026`
- ` 3 ??: ?=?; =??  ??; ?=??|-2026|-2026`

?: ?    ? ,   ? ? UI.  ?  fallback.


## Grid Inspector (Debug Only)

 ?? ??  ? ?? ?? ? ??:

- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-grid-inspector-only --browser-backend openclaw_cdp --tag-selection-mode script`

? ??:
- ??  amoCRM capture;
- ?? ?? ? ?;
- ??  full discovery scan;
-  ??  ? ? grid ? `exports/debug`.


## Google Sheets API: Verified Baseline (2026-04-03)

A separate API verification run confirmed real read/write access.

Executed:
- `python .\test_google_sheets_api.py`

Confirmed:
- Desktop OAuth browser flow completes successfully.
- Read from tab `analytics_writer_test` works.
- Write operation succeeded (`updatedRange=analytics_writer_test!J25`, `updatedCells=1`).
- Target spreadsheet is real Google-hosted sheet (`spreadsheetId=1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0`).

Implication for writer architecture:
- API-driven Sheets writer is now a confirmed and prioritized path.
- Browser/UI writer remains useful as fallback/debug, but should not be long-term primary path.

Planned transition:
1. Introduce dedicated API writer layer.
2. Read layout and ranges through API.
3. Resolve stage-block anchors via API metadata/values.
4. Write stage metrics into resolved cells/ranges.
5. Keep OpenClaw/UI flow for diagnostics and exceptional fallback.


## API Layout Discovery Inspector (Read-Only)

New isolated mode:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-inspector-only`

Scope of this mode:
- Google Sheets API only (read/discovery);
- no writes to sheet;
- no amoCRM scenario execution;
- no browser grid discovery dependency.

Discovery output includes:
- scanned ranges;
- DSL candidate rows;
- header candidate rows;
- accepted/rejected anchors;
- bounded stop reason.

Debug files:
- `exports/debug/layout_api_discovery_reads_<ts>.json`
- `exports/debug/layout_api_discovery_summary_<ts>.txt`

Status:
- production writer is not switched to API write path yet;
- browser/OpenClaw writer path remains intact.


## API Write Path (Opt-in)

New opt-in mode writes stage values through Google Sheets API:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-write`

Dry-run:
- `python -m src.run_profile_analytics --report-id analytics_tag_single_example --writer-layout-api-write --writer-layout-api-dry-run`

Notes:
- API write uses discovered anchors and batch value updates.
- Browser/OpenClaw layout writer remains available as fallback/debug path.
- This step updates only stage value cells (`all/active/closed`) in selected block.

- Debug screenshots are best-effort: screenshot timeout/errors no longer abort analytics capture or API layout dry-run/write planning.


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

## API Preferred Production Routing (2026-04-04)

For destination kind `google_sheets_layout_ui`, production routing can now prefer API write:
- browser/OpenClaw capture builds compiled artifacts;
- API discovery + API layout write executes next;
- UI layout writer remains fallback/debug.

CLI:
- enable preferred routing: `--writer-layout-api-preferred`
- optional fallback to UI writer on API failure: `--writer-layout-api-fallback-to-ui`
- full dry-run capture + API plan: `--writer-layout-api-preferred --writer-layout-api-dry-run`

Backwards compatibility:
- isolated mode `--writer-layout-api-write-from-latest-compiled --writer-layout-api-write` is unchanged;
- explicit opt-in `--writer-layout-api-write` is unchanged.

## Batch From Sheet DSL (API Layout)

New modes:
- `--writer-layout-api-batch-from-sheet-dsl-dry-run`
- `--writer-layout-api-batch-from-sheet-dsl`

Flow:
1. API discovery reads anchors + DSL rows from sheet.
2. For each anchor DSL text, parser builds execution inputs (`tag` / `utm_exact` / `utm_prefix`).
3. Dry-run mode logs parsed mapping only (no amoCRM capture, no writes).
4. Production batch mode runs per-block amoCRM scenario execution, builds per-block compiled artifacts, then writes to exact `dsl_row` block via API writer.

This solves the previous limitation where multiple blocks reused one compiled dataset.

## Browser Filter Handlers v1 (2026-04-09)

The browser capture layer now uses a handler registry for supported filters.

Implemented handler set:
- `tag`
- `pipeline`
- `date`
- `manager`
- `utm_source` exact
- `utm_source` prefix (best-effort)

Contract per handler:
- `resolve(...)`
- `apply(...)`
- `verify(...)`
- `debug_dump(...)`

Notes:
- This update is orchestration-focused: `analytics_flow.py` delegates filter application by key.
- Existing writer/discovery/API paths remain unchanged.
- Tag holder-popup flow remains primary and was preserved as working path.

Still unsupported / partial:
- universal account-agnostic selectors for all amoCRM custom field variants
- full production guarantees for `utm_prefix` across all UI variants
- refusals/AI-summary blocks are out of scope for this handler refactor step

## Runtime Validation Status (2026-04-09)

This section tracks validation state after filter-registry refactor (architecture unchanged).

### Code-level Verified
- Handler routing is active for supported filters v1:
  - `tag`
  - `utm_source` exact
  - `pipeline`
  - `date`
  - `manager`

### Runtime Verified
- `tag` end-to-end runtime is confirmed.

### Runtime Pending
- `utm_source exact` needs repeated runtime confirmation after latest browser-path changes.
- `pipeline` and `date` are currently exercised only as secondary fields in sheet DSL scenarios (not standalone profiles).
- `manager` runtime coverage is missing in current sheet DSL examples.

### Practical Validation Notes
- `batch-from-sheet-dsl-dry-run` is useful for checking DSL parse and routing, but is NOT sufficient to claim standalone runtime verification for `pipeline/date/manager` unless corresponding DSL rows exist and runs complete.


## Execution Input Source vs Writer Destination (2026-04-10)

For `--execution-from-sheet-dsl` there are now two separate targets:

- Execution DSL source target: where DSL anchors are read for runtime override.
  - Config: `report_profiles.yaml -> execution_input.target_id`
  - Optional CLI override: `--execution-source-target-id`
- Writer destination target: where results are written.
  - Config: `report_profiles.yaml -> output.target_id`

Execution override fields from DSL:
- `source_kind`
- `filter_values`
- `tabs` (if present in DSL)
- `filter_operator` (`=` / `^=`)

If execution source target is not configured, runtime falls back to writer destination and logs a warning.

