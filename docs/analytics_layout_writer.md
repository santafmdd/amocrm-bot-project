## Update (2026-04-12): OAuth Behavior and Safe Startup

Google Sheets API auth is separate from amoCRM UI automation:
- OpenClaw CDP is used for amoCRM browser automation only.
- Google OAuth desktop flow (if required) opens OS default browser by design.

To avoid unexpected OAuth popup in normal runs:
- Use cached-token mode: `GOOGLE_API_AUTH_MODE=cache_only`.
- Run explicit bootstrap only when needed: `GOOGLE_API_AUTH_MODE=interactive_bootstrap`.

Auth artifact locations:
- credentials file: `credentials.json` or `GOOGLE_API_CREDENTIALS_FILE`
- token cache: `token.json` or `GOOGLE_API_TOKEN_FILE`

Operational recommendation:
1. Bootstrap token once (interactive mode).
2. Run production/dry-run with `cache_only` mode.
## Update (2026-04-12): Deterministic Anchor Targeting

### New selector
- `--writer-layout-api-target-dsl-cell <A1-ref>`
- Works for both isolated API write-from-latest-compiled and batch-from-sheet-dsl modes.
- Intended for layouts with multiple anchors on same row (for example `A1` and `F1`).

### Discovery/selection guarantees
- Anchors are ordered deterministically by `(dsl_row, dsl_col)`.
- Discovery result stores `dsl_cell` for every anchor.
- Writer selection logs include selector and resolved anchor cell/row.

### Stop behavior
- `cell_read_hard_limit` is kept as safety fuse only.
- Effective hard limit is auto-raised above configured scan budget to avoid premature stop.
- Normal stop reasons should be structural/range-based rather than accidental hard-limit exhaustion.

### Dry-run contract
- `--writer-layout-api-batch-from-sheet-dsl-dry-run` remains strictly non-writing.
- Discovery + parsing + planning + diagnostics are allowed; Sheets value updates are not.
## Update (2026-04-12): Full-Sheet Geometry Discovery

- Discovery no longer assumes only vertical table stack.
- Model:
  1) find DSL command cells,
  2) find all header blocks,
  3) map command -> nearest valid block,
  4) persist row/column bounds per anchor.
- Side and bottom tables are supported when they are inside metadata-bounded scan range.

## Dry-Run Safety (Batch DSL)

- In `--writer-layout-api-batch-from-sheet-dsl-dry-run`, writer receives `dry_run=true` for every anchor.
- Mode may run scenario execution and build write plans/artifacts, but must not call real Sheets value update.

## Update (2026-04-12): Batch Dry-Run Safety

- `--writer-layout-api-batch-from-sheet-dsl-dry-run` must never call real Sheets update path.
- Flow in dry-run: discovery -> DSL parse -> scenario execution -> API write plan -> summary/debug artifacts.
- API writer is invoked with `dry_run=true` and returns plan-only response.

## UTM Prefix Runtime Behavior

- DSL `utm_source^=...` maps to `utm_prefix` runtime mode.
- Current implementation is best-effort exact-entry/selection through UI control, not guaranteed native prefix operator.

## UTM Prefix Runtime Note

- DSL operator `^=` maps to `utm_prefix` execution mode.
- In amoCRM UI this is implemented as best-effort deterministic entry/selection in the discovered control.
- If account-specific UI does not provide reliable prefix semantics, block execution reports controlled failure.


## Update (2026-04-11): Batch DSL Input Integrity

- API discovery stores/uses raw UTF-8 DSL text from Google Sheets anchors.
- Routing no longer performs lossy cp1251/cp866 recoding attempts.
- Date/period DSL values are canonicalized before scenario filter handlers:
  - `????=???????` -> `created`
  - `????=???????` -> `closed`
  - `??????=?? ??? ?????` -> `all_time`
- Date handler diagnostics now include:
  - raw incoming values
  - normalized values
  - current DOM mode
  - current displayed period
  - preset value (`filter[date_preset]`)
  - already-matched decision

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

`google_sheets_layout_ui` РІР‚вЂќ Р С•РЎРѓР Р…Р С•Р Р†Р Р…Р С•Р в„– writer Р Т‘Р В»РЎРЏ stage-Р В±Р В»Р С•Р С”Р С•Р Р† Р Р† РЎРѓРЎС“РЎвЂ°Р ВµРЎРѓРЎвЂљР Р†РЎС“РЎР‹РЎвЂ°Р ВµР С РЎв‚¬Р В°Р В±Р В»Р С•Р Р…Р Вµ Google Sheets.

Р СћР ВµР С—Р ВµРЎР‚РЎРЉ writer Р Т‘Р ВµР В»Р В°Р ВµРЎвЂљ РЎР‚Р ВµР В°Р В»РЎРЉР Р…РЎвЂ№Р в„– per-scenario execution (`||`) РЎвЂЎР ВµРЎР‚Р ВµР В· amoCRM UI Р С—Р ВµРЎР‚Р ВµР Т‘ Р В·Р В°Р С—Р С‘РЎРѓРЎРЉРЎР‹ Р В±Р В»Р С•Р С”Р В°.

## Runtime Flow

1. Р СџР С•Р В»РЎС“РЎвЂЎР С‘РЎвЂљРЎРЉ compiled analytics (`all/active/closed`) Р С”Р В°Р С” baseline.
2. Р СњР В°Р в„–РЎвЂљР С‘ DSL-РЎРѓРЎвЂљРЎР‚Р С•Р С”Р С‘ Р С‘ stage-Р В±Р В»Р С•Р С”Р С‘ Р Р…Р В° Р В»Р С‘РЎРѓРЎвЂљР Вµ.
3. Р вЂќР В»РЎРЏ Р В±Р В»Р С•Р С”Р В° РЎРѓ DSL:
   - Р Р†РЎвЂ№Р С—Р С•Р В»Р Р…Р С‘РЎвЂљРЎРЉ Р Р†РЎРѓР Вµ scenario Р С‘Р В· `||` РЎвЂЎР ВµРЎР‚Р ВµР В· `ScenarioExecutor`;
   - Р С”Р В°Р В¶Р Т‘РЎвЂ№Р в„– scenario Р С—РЎР‚Р С•Р С–Р С•Р Р…РЎРЏР ВµРЎвЂљРЎРѓРЎРЏ Р С•РЎвЂљР Т‘Р ВµР В»РЎРЉР Р…Р С• (reset/apply/capture);
   - Р Р†РЎвЂ№Р В±РЎР‚Р В°РЎвЂљРЎРЉ Р В»РЎС“РЎвЂЎРЎв‚¬Р С‘Р в„– scenario Р С—Р С• score.
4. Р СџР С•РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљРЎРЉ block-specific pivot Р С‘Р В· best scenario.
5. Р вЂ”Р В°Р С—Р С‘РЎРѓР В°РЎвЂљРЎРЉ РЎвЂљР С•Р В»РЎРЉР С”Р С• РЎвЂЎР С‘РЎРѓР В»Р С•Р Р†РЎвЂ№Р Вµ РЎРЏРЎвЂЎР ВµР в„–Р С”Р С‘ `Р вЂ™РЎРѓР Вµ/Р С’Р С”РЎвЂљР С‘Р Р†Р Р…РЎвЂ№Р Вµ/Р вЂ”Р В°Р С”РЎР‚РЎвЂ№РЎвЂљРЎвЂ№Р Вµ`.

## Scenario Scoring

- Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ `total_count`
- Р В·Р В°РЎвЂљР ВµР С Р В±Р С•Р В»РЎРЉРЎв‚¬Р Вµ `non_empty_stage_rows`
- Р В·Р В°РЎвЂљР ВµР С Р С—Р ВµРЎР‚Р Р†РЎвЂ№Р в„– РЎС“РЎРѓР С—Р ВµРЎв‚¬Р Р…РЎвЂ№Р в„–

## Logs

Р С›Р В¶Р С‘Р Т‘Р В°Р ВµР СРЎвЂ№Р Вµ Р СР В°РЎР‚Р С”Р ВµРЎР‚РЎвЂ№:
- `scenario execution start`
- `scenario filter apply success`
- `scenario result`
- `selected_best_scenario`
- `external per-scenario execution finished`
- `layout planned writes`

## Current Limits

- `dates_mode/period/pipeline` Р С—Р С•Р С”Р В° best-effort UI apply;
- `^=` prefix Р В·Р В°Р Р†Р С‘РЎРѓР С‘РЎвЂљ Р С•РЎвЂљ РЎР‚Р ВµР В°Р В»РЎРЉР Р…Р С•Р в„– Р С—Р С•Р Т‘Р Т‘Р ВµРЎР‚Р В¶Р С”Р С‘ Р С—Р С•Р В»РЎРЏ UI (Р С‘Р Р…Р В°РЎвЂЎР Вµ warning);
- Р Р…Р С‘Р В¶Р Р…РЎРЏРЎРЏ РЎвЂљР В°Р В±Р В»Р С‘РЎвЂ Р В° Р С•РЎвЂљР С”Р В°Р В·Р С•Р Р† Р Р…Р Вµ Р С•Р В±РЎР‚Р В°Р В±Р В°РЎвЂљРЎвЂ№Р Р†Р В°Р ВµРЎвЂљРЎРѓРЎРЏ.


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


## UTM Report Profiles: Legacy vs Layout (2026-04-10)

Profiles are intentionally split to avoid coupling execution DSL source and writer destination.

- `analytics_utm_single_example` (legacy path)
  - output target: `event_top_block_1`
  - keep for legacy/non-layout checks.

- `analytics_utm_layout_example` (layout writer path)
  - execution input target: `analytics_layout_stage_blocks_destination`
  - output target: `analytics_layout_stage_blocks_destination`
  - use for `--execution-from-sheet-dsl` and layout API write.

Examples:
- Dry-run:
  `python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script`
- Real write:
  `python -m src.run_profile_analytics --report-id analytics_utm_layout_example --execution-from-sheet-dsl --writer-layout-api-target-dsl-row 14 --writer-layout-api-write --browser-backend openclaw_cdp --tag-selection-mode script`


## 2026-04-10: Batch Scenario Execution Clarification

Current batch behavior for DSL scenarios:
- Primary source filter operator is honored:
  - `РўРµРіРё=...` -> tag primary
  - `utm_source=...` -> utm exact primary
  - `utm_source^=...` -> utm prefix primary (`utm_prefix` handler)
- Additional filters (`Р’РѕСЂРѕРЅРєР°`, `Р”Р°С‚С‹/РџРµСЂРёРѕРґ`, `РњРµРЅРµРґР¶РµСЂ`, secondary `РўРµРіРё`) are applied strictly.
- If additional filter apply fails, scenario fails with explicit controlled error.
- Unsupported DSL fields also fail explicitly; no silent half-applied scenarios.

## 2026-04-10: Pipeline Handler Runtime Notes

For batch DSL scenario execution, `Р’РѕСЂРѕРЅРєР°=...` now uses dedicated row-scoped pipeline handler diagnostics.
Failure is not masked: scenario fails with controlled error when pipeline selection does not reflect in row/panel state.



## Weekly Refusals (Separate MVP)

`weekly refusals` is a separate scenario from stage layout writer.
It runs from `events_list` page type and uses independent parser/writer modules.

- Capture flow: `src/browser/events_flow.py`
- Parser: `src/parsers/weekly_refusals_parser.py`
- Writer: `src/writers/weekly_refusals_block_writer.py`

This does not replace analytics layout writer; it complements it for refusal-specific blocks and stores structured dataset for next AI stage.

## Update (2026-04-15): Anchor Resolution and Skip Semantics

- Analytics UI layout writer is anchor-driven only:
  - DSL anchors -> block anchors -> header row -> stage rows.
- Missing block anchor no longer crashes whole execution.
- Writer now records block-level skip diagnostics and continues with remaining blocks.
- This keeps bounded scan behavior while improving multi-block resilience.

### Tag Block Alias Routing
- Primary aliases come from current `compiled.filter_values`.
- Generic `tag_block_aliases` are used only when `layout.allow_generic_tag_alias_fallback=true`.
- Recommended production setting for stable per-profile targeting:
  - `allow_generic_tag_alias_fallback: false`
