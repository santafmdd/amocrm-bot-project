# Daily Control MVP (LLM-First)

## Scope

`Дневной контроль` active path is LLM-first.

One row = one manager + one control day + one period.

## Architecture

Modules in `src/deal_analyzer/daily_control/`:

1. `source_reader.py`: reads source rows from `Разбор звонков`.
2. `day_grouper.py`: groups by `period_start/period_end/control_day_date/manager_name` and computes counts/mixes.
3. `roks_oap_resolver.py`: resolves current/previous ROKS OAP sheets by period end month.
4. `roks_oap_parser.py`: parses manager metrics from fixed block layout.
5. `daily_analyzer.py`: LLM-first analytics (one request per manager-day).
6. `validation/text_lint.py`: blocker/warning text lint with business-term allowlist.
7. `validation/language_repair.py`: row-level language repair and quarantine.
8. `validation/writer_preflight.py`: row blockers vs batch blockers, partial-write policy.
9. `idempotency.py`: base/exact identity keys and count relation.
10. `writer_plan.py`: deterministic writer plan payload.
11. `sheets_writer.py`: Google Sheets adapter (dry-run/real-write).
12. `artifacts.py`: run artifact persistence.
13. `cli.py`: discover/build/write orchestration.

Legacy wrappers:
- `src/deal_analyzer/daily_control_cli.py`
- `src/deal_analyzer/daily_control_builder.py`
- `src/deal_analyzer/daily_control_writer.py`

## LLM Contract

LLM returns strict JSON (no markdown) per manager-day.
User-facing fields must be Russian. If data is limited, model must state limitations without fabricating facts.

Default routing for daily-control:
- main model: configurable (recommended `qwen3.5:397b-cloud`)
- fallback model: configurable (recommended `deepseek-v3.1:671b-cloud`)

Retry chain:
- `main`
- `main_repair`
- `main_compact_retry`
- `fallback`
- `fallback_repair`

If all attempts fail, row is marked `quarantined_llm_failed` and excluded from writer payload in strict preflight mode.

## Language Lint and Repair

Blockers:
- foreign greeting,
- chinese text,
- markdown fence,
- long foreign-language text in narrative fields.

Warnings:
- allowed Latin business terms,
- technical terms in user-facing text.

Allowlist (not blockers):
- `LINK`, `INFO`, `PLM`, `CRM`, `amoCRM`, `ID`, `URL`, `http`, `https`, `API`, `JSON`, `LLM`, `STT`, `ROKS`, `OAP`.

Before writer preflight, row-level language repair runs:
- deterministic technical cleanup first,
- optional LLM repair if still blocked,
- unrepaired rows go to quarantine.

Artifacts:
- `daily_control_language_repair.json`
- `daily_control_language_repair.md`
- `daily_control_quarantine.json`

## Idempotency and Update Policy

Base key:
- `period_start|period_end|control_day_date|manager_name`

Exact key:
- base key + `sample_size|deals_count|calls_count`

Rules:
- exact match -> skip duplicate,
- same base + same counts -> skip duplicate,
- same base + bigger counts -> update existing row,
- same base + smaller counts -> stale skip,
- weird mismatch -> conflict for review.

## ROKS OAP Parsing

For period ending `2026-04-24`:
- current month: `РОКС ОАП-апрель 2026`
- previous month: `РОКС ОАП-март 2026`

Parser reads fixed manager blocks and metrics including fallback sum for `Дозвоны` from weekly fact columns when monthly fact is empty.

## Writer Safety

`writer_preflight` separates:
- batch blockers (sheet access/schema/plan/global errors),
- row blockers (row payload/language issues).

Defaults for daily-control:
- `strict_preflight=true`
- `allow_partial_write=true`
- `quarantine_unrepaired=true`

## Required Artifacts

Under `workspace/daily_control/<run_id>/`:
- `daily_control_input_groups.json`
- `daily_control_llm_requests.json`
- `daily_control_llm_responses.json`
- `daily_control_payload.json`
- `daily_control_quality_review.json`
- `daily_control_language_repair.json`
- `daily_control_language_repair.md`
- `daily_control_quarantine.json`
- `roks_oap_snapshot.json`
- `daily_control_writer_plan.json`
- `daily_control_writer_plan.md`
- `daily_control_writer_status.json`
- `summary.json`
- `summary.md`

## Commands

Discover:

```powershell
python -m src.deal_analyzer.daily_control.cli discover --config <config> --workbook "РОКС 2026" --daily-sheet "Дневной контроль"
```

Build dry-run:

```powershell
python -m src.deal_analyzer.daily_control.cli build --config <config> --period-start 2026-03-30 --period-end 2026-04-24 --source-sheet "Разбор звонков" --daily-sheet "Дневной контроль" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Write dry-run:

```powershell
python -m src.deal_analyzer.daily_control.cli write --config <config> --run-dir <run_dir> --daily-sheet "Дневной контроль" --dry-run --strict-preflight --allow-partial-write --quarantine-unrepaired
```

Real-write must be run only by explicit operator command after dry-run artifact review.

## Production Routing Note (2026-04-27)

- `Разбор звонков` production real-write is fixed to DeepSeek profile:
  - `config/deal_analyzer.call_review.deepseek.realwrite.json`
  - `ollama_model=qwen3.5:397b-cloud`
- `Дневной контроль` production path uses DeepSeek v4 pro as main (fallback DeepSeek v4 flash).
- `Разбор звонков` берет production модель из конфига; daily/weekly контуры можно переопределить через `--main-model` / `--fallback-model`.
- `call_review_llm_replay` with Gemma is experimental:
  - dry-run is allowed,
  - `--write` with gemma requires `--allow-experimental-gemma-write`,
  - without this flag write is blocked.

## Weekly Cycle Dry-Run Commands

Week plan build with separated signal period and target plan week:

```powershell
python -m src.deal_analyzer.week_plan.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --signal-start 2026-04-20 --signal-end 2026-04-26 --plan-week-start 2026-04-27 --plan-week-end 2026-05-03 --daily-sheet "Дневной контроль" --target-sheet "План недели" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Weekly manager summary build:

```powershell
python -m src.deal_analyzer.weekly_manager_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Дневной контроль" --plan-sheet "План недели" --target-sheet "Недельный свод менеджеров" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

Week summary build:

```powershell
python -m src.deal_analyzer.week_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Дневной контроль" --plan-sheet "План недели" --manager-summary-sheet "Недельный свод менеджеров" --target-sheet "Свод недели" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
```

