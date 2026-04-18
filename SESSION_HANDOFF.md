## Update (2026-04-13): Weekly Refusals Search-Kind Filter Contract

- `Типы событий` uses `checkboxes-search` widget.
- For search-kind, valid scope may be control self-root (`control_self_scope`), not only descendant node.
- Search-kind core selectors:
  - open-state: `.checkboxes-search__opening-list`, `.checkboxes-search__search-input`, `.checkboxes-search__section-common`, `.checkboxes-search__item-label`, `input[type='checkbox'][data-value]`
  - option pick: `.checkboxes-search__item-label:has-text(...)`, `input[data-value='...']` + nearest label container
  - apply: `.js-checkboxes-search-list-apply` / `.checkboxes-search__buttons-wrapper .button-input` / `OK/ОК`
- Page-wide generic `label/checkbox` picking is unsafe for `event_type` (can hit left preset list).
- Search-failure debug artifacts: `weekly_refusals_event_type_search_failed_<ts>.*` with fields `control_class`, `scope_reason`, `checkbox_kind`, `scoped_visible_texts`, `search_root_detected`, `checkbox_search_debug_snapshot`.


## Update (2026-04-13): Weekly Refusals Search Widget Scope Fix

- `entity` stage remains passing.
- `event_type` blocker focus moved to `checkboxes-search` open/scope detection mismatch.
- Implemented search-specific scope/open improvements:
  - search scope can now resolve to control self-root (`control_self_scope`) when root classes indicate `checkboxes-search`.
  - open-state checks now support scoped selectors for `checkboxes-search` internals.
  - option resolution now prioritizes `checkboxes-search__item-label` and `input[data-value]` paths.
  - search apply supports `js-checkboxes-search-list-apply` (including div-based button-like elements).
- Added diagnostics fields on search failures: `control_class`, `scope_reason`, `checkbox_kind`, `scoped_visible_texts`, `search_root_detected`.

## Update (2026-04-13): Weekly Refusals Event Type Search Diagnostics

- `entity` stage currently passes on runtime.
- Current blocker remains `event_type` (`kind=search`) when popup-open detection returns `popup_opened=false`.
- Added targeted diagnostics for search-widget fail-path (`checkbox_popup_not_opened`):
  - `document.activeElement` metadata,
  - visible checkbox/search/dropdown class elements,
  - visible `OK/ОК` buttons,
  - visible elements containing event-type texts (`????????? ????? ???????`, `????? ??????`, `?????? ???????`),
  - outerHTML snippets + class + text + bbox.
- Next step: compare captured live DOM snapshot for `Типы событий` popup and align open/wait selectors to that shape.

## Update (2026-04-13): Weekly Refusals Entity Regression After Checkbox Refactor

- Runtime regression was confirmed again on `entity` (`??? ????????`) after generalized checkbox changes:
  - option click succeeds (`??????`),
  - strict immediate option re-check may fail (`found=false`) due to DOM rerender/collapse,
  - stage failed too early.
- Checkbox-like controls are now treated with explicit contracts:
  - `dropdown` (`checkboxes_dropdown`) uses resilient verification (option state OR reflected control/input/chip OR checked snapshot, optional one-time reopen-check).
  - `search` (`checkboxes-search`) keeps scoped selection and now finalizes via explicit `OK` click before close.
- Weekly refusals remains on real `ui_controls` path (no forced preset fallback).
- Next runtime chain after restoring stable entity/event_type: `pipeline -> status_before -> status_after -> apply -> parse rows`.

## Update (2026-04-13): Weekly Refusals Event Type (`checkboxes-search`)

- Entity stage (`??? ????????`) is stable via checkbox UI path.
- Current blocker moved to `event_type` because `Типы событий` uses `checkboxes-search` widget, not `checkboxes_dropdown`.
- Added generalized checkbox-like control handling with explicit kinds:
  - `dropdown` (`checkboxes_dropdown`)
  - `search` (`checkboxes-search`)
- `checkboxes-search` now uses scoped selection/verification container; full-page option clicking is rejected for this path.
- Saved preset remains optional only; weekly flow continues with real `ui_controls` path.
- Next runtime chain after event_type: `pipeline -> status_before -> status_after -> apply -> parse rows`.

## Update (2026-04-13): Weekly Refusals Checkbox Verification Contract

- Runtime blocker moved from checkbox open to post-select verification (`??? ???????? -> ??????`).
- Checkbox path now verifies selected state while dropdown is still open (checked/aria/class/data markers).
- For checkbox controls, primary success is selected-state in dropdown; control text reflection after `Escape` is secondary.
- Weekly profiles keep `filter_mode=ui_controls` as primary path; saved preset routing remains optional and is not promoted as default.
- Next runtime target chain remains: `event_type -> pipeline -> status_before -> status_after -> apply -> parse rows`.

## Update (2026-04-13): Weekly Refusals Checkbox Dropdown Pragmatic Fix

- Blocker moved from panel-open to entity selector apply (`??? ???????? -> ??????`) in amoCRM `checkboxes_dropdown` widget.
- Added pragmatic special-case path for checkbox dropdown controls:
  - open via `.checkboxes_dropdown__title_wrapper` first,
  - value select via checkbox/label rows,
  - close via `Escape` (no forced OK for this control type).
- Generic select logic is preserved as fallback for non-checkbox controls.
- Added optional weekly profile routing fields:
  - `filter_mode` (`ui_controls` default, `saved_preset` optional)
  - `saved_preset_name`
  - `saved_preset_exact_match`
- Saved-preset mode is now routed in weekly flow (`apply preset` + `date refresh`).
- Hybrid recommendation for MVP weekly reports:
  - use `saved_preset` for repetitive stable filters,
  - keep `ui_controls` for autonomous/future analysis scenarios.

## Update (2026-04-13): Weekly Refusals Entity Control Apply/Verification

- `events/list` opener and panel-open paths are stable.
- Current blocker was narrowed to entity apply stage (`??? ???????? -> ??????`) due to container-level click without real control target.
- `EventsFlow` control apply now resolves clickable descendants inside control container first; parent-only click is no longer primary.
- Added right-side bbox fallback click for select-like controls when clickable descendant is not found.
- Added popup diagnostics after control click: `popup_opened`, visible options, control text before/after.
- Added broader option pick selectors for checkbox/pseudo-option rows (`label/li/role=option` variants).
- Selection verification now accepts any confirmed reflection source:
  - `control_text`,
  - `input_value`,
  - `chip_text`.
- Weekly profile config now stores non-empty `status_before_values` for 2m/long weekly and cumulative profiles.
- Weekly period strategy remains:
  - Sunday = `?? ??? ??????`
  - otherwise = `?? ??????? ??????`

## Update (2026-04-13): Weekly Refusals Filter Control Resolver

- `events/list` filter opener remains fixed; current blocker moved into control resolution inside opened panel.
- Fixed overmatch path in `EventsFlow._apply_control_values(...)`: no blind first-match from broad `*:has-text(...)`.
- Added `_resolve_filter_control(panel, control_label)` with candidate ranking and control-container promotion.
- Added popup confirmation support for multi-select controls with `OK/ОК` buttons.
- Added/updated diagnostics per stage (`entity/event_type/status_before/status_after/apply`) via `weekly_refusals_<stage>_failed_<ts>.*` artifacts.
- Weekly period strategy is now explicit:
  - Sunday -> `?? ??? ??????`
  - Monday-Saturday -> `?? ??????? ??????`

﻿## Update (2026-04-12): Google Auth UX Guard

Root cause of confusing browser popup:
- OAuth desktop flow can open system default browser when token cache is missing/invalid.
- This is expected Google OAuth behavior and is not related to OpenClaw amoCRM browser session.

Implemented guardrails:
- Added auth mode selection via `GOOGLE_API_AUTH_MODE`:
  - `auto`, `cache_only`, `interactive_bootstrap`.
- Runtime now logs selected auth mode and auth decision path.
- Cached token/refresh token path is preferred and logged.
- In `cache_only` mode no browser popup is allowed; command fails with explicit bootstrap instruction.

Key logs:
- `google auth mode selected: ...`
- `google auth: using cached token`
- `google auth: refresh token updated`
- `google auth: interactive authorization required (system browser OAuth flow)`
- `google auth: interactive authorization disabled (mode=cache_only) reason=...`
## Update (2026-04-12): Discovery Stop + Targeted Anchor Selection

- Added deterministic anchor sorting by `(dsl_row, dsl_col)` for API batch/discovery paths.
- Added exact block selector: `--writer-layout-api-target-dsl-cell` (example: `F1`).
- API discovery payload now includes `dsl_cell` for each anchor; summaries/logs show selected cell explicitly.
- Discovery stop logic hardened:
  - `cell_read_hard_limit` is now a high safety fuse,
  - default behavior scans configured range and stops structurally (`scan_range_exhausted`, `anchors_found_limit_reached`, etc.),
  - safer for future lower blocks after large row gaps.
- Dry-run safety preserved: no real Sheets updates in batch dry-run path.
## Update (2026-04-12) ? Discovery Geometry + Strict Dry-Run

- API discovery refactored to generalized block geometry:
  - independent DSL candidate detection,
  - independent header detection,
  - mapping by nearest valid table geometry,
  - anchor bounds persisted (row/col ranges + topology).
- Batch dry-run contract hardened:
  - dry-run keeps execution and planning,
  - real sheet update path is forbidden (`dry_run=true` propagated into API writer).

## Update (2026-04-12) ? Batch Dry-Run Write Guard

- Fixed contract bug: batch dry-run no longer performs real Google Sheets writes.
- `_run_api_layout_batch_from_sheet_dsl` now always forwards runtime `dry_run` into API writer.
- Dry-run keeps scenario execution and write planning enabled for realistic diagnostics.
- Dry-run summary rows: `dry_run_planned`, `planned_updates`, `updated_cells_count=0`.

## UTM Prefix Note

- `utm_source^=` support is best-effort via available UI control entry/selection.
- This is not equivalent to guaranteed server-side/native prefix operator behavior.

## UTM Prefix Contract (Current)

- `utm_source^=` is supported as a routed execution mode (`utm_prefix`).
- Runtime behavior is best-effort through available UI controls, not a guaranteed native prefix operator.
- Success/failure is logged explicitly; no silent fallback to fake success.


## Update (2026-04-11) ? DSL UTF-8 + Date Canonicalization

- Fixed DSL parsing aliases back to UTF-8 canonical names (`????`, `utm_source`, `???????`, `????`, `??????`, `?`, `??`).
- Removed destructive mojibake "repair" path in API layout discovery/routing; discovery now keeps raw UTF-8 text from Google Sheets.
- Added canonical date normalization before handler apply in scenario execution:
  - `???????/created -> created`
  - `???????/closed -> closed`
  - `?? ??? ?????/all time -> all_time` (plus other period presets)
- Date handler now verifies by normalized state (mode + period), and treats already-matched state as success.
- Added diagnostics in date handler for raw/normalized inputs and detected current widget state.


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

Layout writer РЎвЂљР ВµР С—Р ВµРЎР‚РЎРЉ РЎС“Р СР ВµР ВµРЎвЂљ Р Р…Р Вµ РЎвЂљР С•Р В»РЎРЉР С”Р С• Р С—Р В°РЎР‚РЎРѓР С‘РЎвЂљРЎРЉ DSL, Р Р…Р С• Р С‘ Р В·Р В°Р С—РЎС“РЎРѓР С”Р В°РЎвЂљРЎРЉ per-scenario execution Р Т‘Р В»РЎРЏ `||` РЎвЂЎР ВµРЎР‚Р ВµР В· РЎР‚Р ВµР В°Р В»РЎРЉР Р…РЎвЂ№Р в„– UI amoCRM.

## What Was Added

1. New execution layer:
- `src/analytics/scenario_executor.py`
- Р С›РЎРѓР Р…Р С•Р Р†Р Р…Р В°РЎРЏ РЎвЂљР С•РЎвЂЎР С”Р В°: `ScenarioExecutor.execute_block_scenarios(...)`

2. End-to-end scenario run per block:
- Р В±Р ВµРЎР‚РЎвЂР С DSL Р В¶РЎвЂР В»РЎвЂљР С•Р в„– РЎРѓРЎвЂљРЎР‚Р С•Р С”Р С‘;
- РЎР‚Р В°Р В·Р В±Р С‘Р Р†Р В°Р ВµР С Р Р…Р В° `scenarios[]`;
- Р Т‘Р В»РЎРЏ Р С”Р В°Р В¶Р Т‘Р С•Р С–Р С• РЎРѓРЎвЂ Р ВµР Р…Р В°РЎР‚Р С‘РЎРЏ Р С•РЎвЂљР Т‘Р ВµР В»РЎРЉР Р…Р С•:
  - reset to clean analytics state;
  - open filter panel;
  - apply filters scenario;
  - apply and wait;
  - capture `all/active/closed` snapshots;
  - РЎРѓРЎвЂЎР С‘РЎвЂљР В°РЎвЂљРЎРЉ score (`total_count`, `non_empty_stage_rows`).

3. Best scenario selection:
- max `total_count`
- tie -> max `non_empty_stage_rows`
- tie -> first successful

4. Layout writer integration:
- `src/writers/google_sheets_layout_ui_writer.py`
- Р ВµРЎРѓР В»Р С‘ РЎС“ Р В±Р В»Р С•Р С”Р В° Р ВµРЎРѓРЎвЂљРЎРЉ DSL Р С‘ Р С—Р ВµРЎР‚Р ВµР Т‘Р В°Р Р… executor:
  - Р Р†РЎвЂ№Р С—Р С•Р В»Р Р…РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ РЎР‚Р ВµР В°Р В»РЎРЉР Р…РЎвЂ№Р Вµ scenario runs;
  - Р В±Р ВµРЎР‚РЎвЂРЎвЂљРЎРѓРЎРЏ best scenario result;
  - РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљРЎРѓРЎРЏ block-specific pivot;
  - Р Р† РЎвЂљР В°Р В±Р В»Р С‘РЎвЂ РЎС“ Р С—Р С‘РЎв‚¬РЎС“РЎвЂљРЎРѓРЎРЏ Р В·Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘РЎРЏ РЎвЂљР С•Р В»РЎРЉР С”Р С• best scenario.

5. Entrypoint integration:
- `src/run_profile_analytics.py`
- Р С—РЎР‚Р С‘ `destination.kind == google_sheets_layout_ui` РЎРѓР С•Р В·Р Т‘Р В°РЎвЂРЎвЂљРЎРѓРЎРЏ `ScenarioExecutor`
  Р С‘ Р С—Р ВµРЎР‚Р ВµР Т‘Р В°РЎвЂРЎвЂљРЎРѓРЎРЏ Р Р† layout writer.

6. Tests:
- `tests/test_scenario_executor.py` (selection/merge-level checks)

## Current Practical Limits

- Р В¤Р С‘Р В»РЎРЉРЎвЂљРЎР‚РЎвЂ№ `pipeline/period/dates_mode` Р С—РЎР‚Р С‘Р СР ВµР Р…РЎРЏРЎР‹РЎвЂљРЎРѓРЎРЏ best-effort РЎвЂЎР ВµРЎР‚Р ВµР В· label-driven UI path.
- Р вЂќР В»РЎРЏ `^=` prefix Р Р† UI Р С—Р С•Р С”Р В° РЎвЂЎР ВµРЎРѓРЎвЂљР Р…РЎвЂ№Р в„– best-effort (РЎРѓ warning, Р ВµРЎРѓР В»Р С‘ Р С—Р С•Р В»Р Вµ/Р СР ВµРЎвЂ¦Р В°Р Р…Р С‘Р С”Р В° Р Р…Р Вµ Р Р…Р В°Р в„–Р Т‘Р ВµР Р…РЎвЂ№).
- Р СњР С‘Р В¶Р Р…РЎРЏРЎРЏ РЎвЂљР В°Р В±Р В»Р С‘РЎвЂ Р В° Р С•РЎвЂљР С”Р В°Р В·Р С•Р Р† Р С—Р С•-Р С—РЎР‚Р ВµР В¶Р Р…Р ВµР СРЎС“ out-of-scope.

## Next Step

1. Р Р€РЎРѓР С‘Р В»Р С‘РЎвЂљРЎРЉ field-specific apply Р Т‘Р В»РЎРЏ `dates_mode/period/pipeline` (UI selectors per real account).
2. Р вЂќР С•Р В±Р В°Р Р†Р С‘РЎвЂљРЎРЉ Р В±Р С•Р В»Р ВµР Вµ РЎРѓРЎвЂљРЎР‚Р С•Р С–РЎС“РЎР‹ Р Р†Р В°Р В»Р С‘Р Т‘Р В°РЎвЂ Р С‘РЎР‹ Р С—РЎР‚Р С‘Р СР ВµР Р…РЎвЂР Р…Р Р…Р С•Р С–Р С• РЎвЂћР С‘Р В»РЎРЉРЎвЂљРЎР‚Р В° Р С—Р ВµРЎР‚Р ВµР Т‘ capture.
3. Р В Р В°РЎРѓРЎв‚¬Р С‘РЎР‚Р С‘РЎвЂљРЎРЉ diagnostics per scenario (structured summary + screenshot map).


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
  - `...; РњРµРЅРµРґР¶РµСЂ=<name>; РўРµРіРё=<value>`
  or
  - `...; РњРµРЅРµРґР¶РµСЂ=<name>; utm_source=<value>`
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

## Update (2026-04-10) - Apply Fallback Hardening (analytics browser flow)

### Root Cause
- `AnalyticsFlow._click_apply_in_panel(...)` called a missing helper: `_dump_apply_button_diagnostics(...)`.
- On pointer interception (for example, overlay like MANGO OFFICE) normal apply click could fail, and diagnostics path crashed with `AttributeError`.
- Post-click confirmation was too brittle (single short wait + single check).

### Fix Implemented
- Added `AnalyticsFlow._dump_apply_button_diagnostics(...)`.
  - Saves both text and JSON candidate dumps to `exports/debug`.
  - Best-effort only: diagnostics never aborts business flow.
- Hardened `_click_apply_in_panel(...)`:
  - diagnostics calls are wrapped and cannot break apply flow;
  - keeps click strategy order: `normal -> force -> js`;
  - apply confirmation now uses short polling window (~2s total, 200ms steps), not a single immediate check.

### Apply Confirmation Signals (current)
Apply is considered confirmed when at least one is observed:
- URL markers changed (`useFilter` / filter marker via existing URL check), or
- filter panel/overlay is closed, or
- URL changed after click.

### Validation Scope
- Fix targets browser apply fallback only.
- No writer/discovery/DSL logic changes in this step.

## Update (2026-04-10) - Config Mojibake Cleanup

- Apply fallback crash fix is closed (missing apply diagnostics helper + brittle confirmation addressed).
- Next cleanup step completed for config path: removed `????????` placeholder from `config/report_profiles.yaml` active example profile values.
- `suspicious_entries=['????????']` warning is no longer expected for current report profiles.
- Added config hygiene test to prevent reintroducing `???` placeholders in `report_profiles.yaml`.

## Update (2026-04-10) - Batch DSL Prefix/Strictness Fix

### Fixed
- Restored missing `AnalyticsFlow._choose_option_text` used by filter handlers in batch scenario execution.
- Primary DSL operator for source filter is now propagated into runtime apply (`=` / `^=`).
- For `utm_source^=...`, batch execution now routes primary apply to `utm_prefix` handler instead of exact-only path.

### Behavior Change (Intentional)
- Scenario execution no longer keeps partial successes for non-primary filters.
- If `pipeline/date/manager/secondary-tag` handler apply fails, scenario gets controlled failure:
  - `Scenario filter apply failed: field=...`
- Unknown DSL field now fails explicitly:
  - `Unsupported DSL filter for scenario execution: field=...`

### Notes
- This keeps already-working tag / utm-exact blocks intact while making prefix handling explicit and deterministic in batch path.

## Update (2026-04-10) - Pipeline Runtime Diagnostics

- Pipeline apply in batch scenario execution is now deterministic row-scoped with verbose diagnostics.
- If pipeline cannot be applied/verified, scenario fails explicitly with controlled message and payload context.
- This keeps hard-fail semantics while exposing exact failure point (row, click-target, options, reflection).



## 2026-04-12 Weekly Refusals MVP Update

Implemented separate `events_list` execution path (no regression in analytics_sales routing expected):
- `src/browser/events_flow.py`
- `src/parsers/weekly_refusals_parser.py`
- `src/writers/weekly_refusals_block_writer.py`
- `run_profile_analytics.py` now routes `page_type=events_list` to weekly refusals runner.

Current capabilities:
- open events list
- apply refusal-specific filters (date/pipeline/status/entity/event type/managers)
- parse event rows
- aggregate before/after statuses
- persist deal refs for future AI stage
- save compiled artifact + write summary

Profiles added:
- weekly_refusals_weekly_2m
- weekly_refusals_weekly_long
- weekly_refusals_cumulative_2m
- weekly_refusals_cumulative_long

Dry-run behavior:
- use `--writer-layout-api-dry-run` (or `--writer-layout-dry-run`) to avoid sheet updates while keeping artifacts.

Known limitations:
- events-list selector set is MVP-level and may require tuning against live amoCRM DOM changes.
- no AI analysis / no deep deal crawling yet.

## Weekly Refusals IDs Sync (2026-04-13)

Documentation/command mismatch fixed.

Canonical weekly refusals report IDs:
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`
- `weekly_refusals_example` (alias smoke profile; equivalent filters/output to weekly_2m)

Recommended smoke dry-run command:
```bash
python -m src.run_profile_analytics --report-id weekly_refusals_example --writer-layout-api-dry-run --browser-backend openclaw_cdp --tag-selection-mode script
```

## Update (2026-04-13): Weekly Refusals Filter-Open Diagnostics

Runtime blocker moved to early browser layer on `events_list`:
- filter panel opening could fail before any refusal parsing/writing.

Implemented in `src/browser/events_flow.py`:
- `_open_filter_panel` now first checks if panel is already open.
- Added readiness wait before selector scan.
- Expanded selector set for text/role/aria/title/data-test/class/toolbar button variants.
- Added per-selector diagnostics in logs:
  - selector candidates list
  - `matched_count` for each candidate
- Added fail artifacts when panel open fails:
  - `exports/debug/weekly_refusals_filter_open_failed_<ts>.png`
  - `exports/debug/weekly_refusals_filter_open_failed_<ts>.json`
  - `exports/debug/weekly_refusals_filter_open_failed_<ts>.txt`
  - `exports/debug/weekly_refusals_filter_open_failed_<ts>.html`
- RuntimeError now includes:
  - `checked_selectors_count`
  - `visible_candidates_count`
  - `current_url`
  - `debug_artifacts_path`

## Update (2026-04-13): Events List Filter Opener Fix

Root blocker in weekly refusals runtime was `events/list` filter opener.
In live amoCRM UI opener is top search-like control with text `Фильтр` + icon,
not always a classic `button`.

Implemented in `src/browser/events_flow.py`:
- panel detection now based on visible labels (>=3 markers):
  - `Менеджеры`, `Все сущности`, `Типы событий`, `Значение до`, `Значение после`, `За все время`
- opener flow now multi-step:
  1) direct selector click (visible candidates only, top-position preferred)
  2) clickable ancestor search/click from `Фильтр` text node
  3) bbox center click fallback
- `[class*='filter'] button` demoted to low-priority fallback (no longer primary opener).

Fail diagnostics extended:
- `weekly_refusals_filter_open_failed_<ts>.png`
- `weekly_refusals_filter_open_failed_<ts>.json`
- `weekly_refusals_filter_open_failed_<ts>.txt`
- `weekly_refusals_filter_open_failed_<ts>.html`
with checked selectors, candidate payloads, marker visibility, URL and explanation.


## Update (2026-04-13): Weekly Refusals Filter Controls Mapping + Apply Variants

- Events/list opener remained fixed; next blocker was control mapping in panel filters before apply.
- Weekly refusals events flow now uses control-specific selection (not generic text input) for:
  - `??? ????????`
  - `Типы событий`
  - `???????`
  - `???????? ??` (multi-select)
  - `???????? ?????`
  - date mode + period control (`.date_filter`)
- Added backward-compatible profile support:
  - `status_before_values: list[str]` (primary)
  - `status_before` (legacy fallback)
  - `period_strategy` field (non-breaking, default `ui_period_control`)
- Apply button logic expanded to variants:
  - `?????????` / `???????` / `??????`
  - click fallback: `normal -> force -> js`
  - confirmation uses short polling (panel close or URL change or rows visible)
- Stage-specific fail diagnostics added to `exports/debug`:
  - `weekly_refusals_entity_failed_<ts>.*`
  - `weekly_refusals_event_type_failed_<ts>.*`
  - `weekly_refusals_status_before_failed_<ts>.*`
  - `weekly_refusals_apply_failed_<ts>.*`

## Update (2026-04-15): Runtime Contracts

### Anchor-only writing (analytics layout)
- `google_sheets_layout_ui_writer` now skips missing blocks instead of aborting full run.
- Skip log includes block name, aliases, reason, and debug artifact paths.
- Each block is processed independently.
- `start_cell` is not used as operational block resolver for analytics layout writer.

### Weekly refusals period control
Runtime options were centralized and can be overridden without code edits:
- `--weekly-period-strategy`
- `--weekly-period-mode`
- `--weekly-date-from`
- `--weekly-date-to`

Supported strategies:
- `current_week`, `previous_week`, `auto_weekly`, `monday_current_else_previous`, `manual_range`.

### Scenario DSL filters (current boundary)
Supported fields:
`tags`, `utm_source`, `pipeline`, `period`, `dates_mode`, `date_from`, `date_to`, `manager`.

Unknown fields are logged as explicit warnings (`unsupported dsl filter field`) and do not fail silently.

## Update (2026-04-16): Weekly Writer Canonicalization + Safe Expansion

- Added canonical refusal status normalization layer (`src/domain/refusal_status_normalizer.py`) with deterministic rules:
  - lowercase/trim,
  - repeated-space collapse,
  - `ё -> е`,
  - soft punctuation cleanup,
  - explicit alias map for confirmed near-duplicates (`...на свя/связ/...`).
- Weekly refusals parser now aggregates after-side values by canonicalized granular reason.
- Weekly refusals writer now performs compact block planning and dedupes canonical duplicates from both parsed input and existing sheet rows.
- If planned compact block exceeds available rows before next section, writer performs real `insert_rows` (Google Sheets API) to expand the block safely.
- Manual columns are still preserved, dry-run still supported, weekly/cumulative routing unchanged.

Known limitation:
- Conservative near-match helper exists, but canonical merge primarily relies on deterministic normalization + explicit aliases.

## Update (2026-04-16): Weekly Refusals Writer Contracts (Operational)

- Weekly refusals writer now expands target block by real Google Sheets row insert when block height is insufficient.
- Next section block is physically shifted down after insert (no overlap risk with lower block).
- Canonical refusal normalization is applied before aggregation and before write planning.
- Confirmed near-duplicate variants are merged into one canonical reason key, including:
  - `???????? ???????? ?? ???`
  - `???????? ???????? ?? ????`
  - `???????? ???????? ?? ?????`
  Result: one canonical row `???????? ???????? ?? ?????` with aggregated count.
- Dry-run behavior is preserved: planning/debug artifacts are produced, but no real values update and no row insert is sent to Sheets.
- Next major stage remains pending: deal/episode analyzer (AI analysis layer) is NOT implemented yet.



## Update (2026-04-16): Cumulative Semantics Validation Pass

- Weekly refusals writer semantics are now explicit and stable:
  - `mode=weekly`: overwrite block counts from source rows for selected weekly slice.
  - `mode=cumulative`: **recompute from source rows for configured cumulative range** and overwrite resulting totals.
- Cumulative mode no longer adds current counts to existing sheet values.
- Writer summary now includes `writer_mode_semantics=recompute_from_source`.
- Compiled weekly artifact now also carries `writer_mode_semantics=recompute_from_source` for traceability.

Manual validation (4 profiles):
- `weekly_refusals_weekly_2m`
- `weekly_refusals_weekly_long`
- `weekly_refusals_cumulative_2m`
- `weekly_refusals_cumulative_long`

Check in logs/summaries:
- `mode=...`
- `semantics=recompute_from_source`
- no additive behavior on rerun of same period.


## Update (2026-04-16): Cumulative Add Mode + Period Idempotency Guard

Weekly/cumulative writer got a new cumulative strategy:
- `cumulative_write_strategy=recompute_from_source` (existing behavior)
- `cumulative_write_strategy=add_existing_values` (new)

Current contracts:
- `weekly`: overwrite counts from current source slice.
- `cumulative + recompute_from_source`: overwrite counts with recomputed source totals.
- `cumulative + add_existing_values`: `existing_sheet_count + incoming_week_count` by canonical key.

Idempotency guard (add mode only):
- guarded by `period_key` per `target_id`;
- duplicate live apply for same `period_key` is blocked with RuntimeError;
- optional override via `cumulative_force_reapply=true` in parsed payload (not enabled by default runtime flags).

Layout planning note:
- right/left block planning is compact; internal empty rows are not used as placement slots.
- ungroupped tail reasons (for example `??? ???????`) are pushed to tail by final order and do not fill internal gaps.


## Update (2026-04-16): Analyzer Design Kickoff

??????? ???????? scope ?? Google Sheets ???????????? ??? ???????????:
- analytics block writer,
- weekly refusals: weekly 2m/long,
- cumulative refusals: 2m/long.

??????? ????????? ???????:
- `weekly`: overwrite-from-source,
- `cumulative`: additive `add_existing_values` (? period idempotency guard).

????????? ???? ???????: ?????????????? ? ?????? ??????????? ??????/????????.

??????? ??????-????? ??????????? ? Google Sheets:
- `???????? ????????`
- `?????? ??????`
- `?????`
- `?????????`

?????:
- ??????????? ????? ???? ?? ????????? ? Google Sheets,
- ???????????? ???????? ???????? (???????/?????????/guard state).

UI backlog (???????????? ??????? controls):
- ????????? weekly/cumulative ???????,
- ???????????? ???????/??????? ??????,
- ?????? ????????? ?? tag/utm_source,
- ????????? ???????????,
- ????????? ?????????? ??? ???????????.

## Update (2026-04-18): Deal Analyzer Enrichment MVP Delivered

Delivered (read-only):
- external enrich from client list and appointment table via Google Sheets API read path;
- deterministic matching priority (`deal_id -> phone -> email -> company+contact -> company`);
- ROKS context extractor for manager/team KPI snapshot with sanitization of formula-noise values;
- unified snapshot builder for analyzer input;
- operator CLI commands:
  - `enrich-deal`
  - `enrich-period`
  - `roks-snapshot`

Guaranteed unchanged:
- analytics flow;
- weekly_refusals flow;
- existing Google Sheets writer flows.

Current limitations (explicit):
- no write-back from enrich pipeline;
- ROKS extraction is marker/header-driven and may require per-sheet mapping hardening for full KPI precision.

Reference doc:
- `docs/deal_analyzer_enrichment_mvp.md`.

## Update (2026-04-18): Analyzer Call Evidence + Transcription MVP

Done:
- read-only call evidence for deal/period (`call_id`, `duration`, `direction`, recording refs, quality flags);
- deterministic call dedup;
- transcription adapter backends (`disabled/mock/local_placeholder/cloud_placeholder`);
- transcript cache with deterministic key;
- snapshot now includes `call_evidence`, `transcripts`, `call_derived_summary`;
- new CLI commands: `collect-calls`, `transcribe-deal`, `transcribe-period`, `build-call-snapshot`.

No regressions introduced into:
- analytics flow
- weekly_refusals flow
- existing Google Sheets writers

Reference doc:
- `docs/deal_analyzer_calls_mvp.md`

## Update (2026-04-18): Storage Janitor MVP

Done:
- new `ops_storage` module with retention planner + janitor cleaner + report renderer;
- policy supports `keep_latest`, `keep last N`, `older-than-days`, `max-size trim`;
- allowlist safety enforced (paths outside allowlist fail fast);
- integrated operator CLI commands `janitor-report` / `janitor-clean`.

Not changed:
- analytics flow
- weekly_refusals flow
- existing writer logic

## Update 2026-04-18: Test Hygiene + Storage Cleanup

- Full test command `python -m pytest -q -p no:cacheprovider tests` is green (no import mismatch).
- Collection issue was caused by duplicate test module basenames across `tests/*` subpackages.
- Added package markers in test subdirs and a dedicated hygiene test.
- Janitor now tracks additional cleanup roots:
  - `workspace/screenshots`
  - `workspace/tmp`
  - `workspace/tmp_tests`
  - `pytest-tmp`
  - `pytest_tmp_env`
- Default retention added for screenshots and tmp dirs; janitor remains opt-in (`janitor_enabled=false` by default).
