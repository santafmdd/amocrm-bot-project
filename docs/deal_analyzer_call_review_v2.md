# Deal Analyzer Call Review V2

## Scope
- Scenario: `analyze-period`
- Target sheet: `Разбор звонков`
- One row = one meaningful call case

## Pipeline
1. Period-wide call metadata pass (before limit)
2. Pool split:
   - `conversation_pool`
   - `discipline_pool`
3. Transcription shortlist from `conversation_pool` only
4. Business-window filter (MSK, cut-off `15:00`)
5. Per-deal analysis + transcript signals
6. LLM layered call-review generation:
   - free form
   - effect layer
   - structured JSON
   - style rewrite
   - final assemble
7. Native call-review writer (append)

## Writer Isolation
- Active writer: `call_review_writer` only.
- `daily_control_writer` and `meeting_queue_writer` are inactive for `analyze-period`.
- Legacy daily payload is kept only as compatibility artifact/debug placeholder.

## LLM Safety
- If no live runtime (`selected=none`) -> forced dry-run, no battle write.
- User-facing columns are LLM-authored only.
- Rules layer is used for selection, diagnostics, guardrails, and safety checks.

## Business Windows
- Window day boundary: `15:00` Moscow time.
- Calls after `15:00` move to next workday bucket.
- Open bucket of current day is excluded from battle write.
- `Дата анализа` = bucket day, `Дата кейса` = anchor-call day.

## Dropdown / Header Contract
- Writer reads real headers from sheet row 1.
- Writer reads validation options on first data row and normalizes values to allowed options.
- If mapping confidence is low for dropdown value, cell is left empty instead of writing invalid token.

## Debug Artifacts
- `call_pool_debug.json/.md`
- `conversation_pool.json/.md`
- `discipline_pool.json/.md`
- `transcription_shortlist.json/.md`
- `analysis_shortlist.json/.md`
- `call_review_step_artifacts/*`
- `call_review_sheet_payload.json`
- `summary.json` (`call_review_writer`, `call_business_windows`, `call_runtime_diagnostics`, `call_review_llm_generation`)
