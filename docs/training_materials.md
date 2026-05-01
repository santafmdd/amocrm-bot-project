# Training Materials

## Назначение
Контур `training_materials` создает обучающие материалы и задачи после обучения по строкам `План недели` (`activity_type=обучение`) и записывает ссылки обратно в лист.

## Входы
- `План недели` (обязательный источник кандидатов).
- `Дневной контроль` и `Разбор звонков` (контекст ошибок/роста).
- style/speech/product sources.
- external internet sources (по политике ниже).

## Model policy
Рекомендуемый `--model-pool`:
- `qwen3.5:397b-cloud`
- `gpt-oss:120b-cloud`
- `deepseek-v3.1:671b-cloud`
- `deepseek-v4-pro:cloud` (premium retry для проблемных строк)

## Role policy integration
- Для `sales_manager` темы и примеры должны быть про warm/current pipeline, demo/test/invoice/payment, renewals/reactivation.
- Для `telemarketer` допустим верх воронки: cold first contact, ЛПР, interest, appointment.
- Для sales_manager исключаем «массовый холодный обзвон» как основную тему обучения.

## Demo methodology alignment
Для sales_manager demo-related материалы должны следовать стандарту:
- educational demo
- guided discovery
- client hands-on
- фиксация next-step и критерия успеха теста

## SMART и quality gates
Ключевые требования:
- структура документа;
- достаточный объем;
- речевые модули;
- чек-лист;
- практические задания;
- связь с evidence/планом.

Важно:
- foreign/business terms в training не должны автоматически блокировать документ;
- URL/названия систем/метрик считаются допустимыми (warning, не blocker, если контекст валиден).

## External sources policy
Флаги build:
- `--require-external-sources` (default strict)
- `--allow-no-external-sources` (разрешить build с warning)
- `--external-search-provider auto`
- `--external-search-limit <N>` (лимит источников, не лимит обучений)
- `--external-source-min-count <N>`

Provider chain (`auto`):
1. live external provider (если доступен)
2. fallback search provider
3. curated fallback (`training_materials_external_sources_file` / env)

Если strict и external недоступны:
- `block_reason=external_sources_unavailable` или `source_coverage_failed`.

## Curated fallback
Конфиг:
- `training_materials_external_sources_file`
- `training_materials_external_curated_urls`
- `training_materials_external_fetch_timeout_seconds`

Рекомендуемый файл:
- `docs/training_materials_external_sources.json`

## Почему источники не показываются в Google Doc
В employee-facing документе источники не выводятся, чтобы материал оставался прикладным.

Источники фиксируются только в artifacts:
- `training_materials_source_coverage.json`
- `training_materials_external_sources_debug.json`
- `summary.json`

## CLI
### Build dry-run
```powershell
python -m src.deal_analyzer.training_materials.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --plan-sheet "План недели" --daily-sheet "Дневной контроль" --call-review-sheet "Разбор звонков" --week-start 2026-04-13 --week-end 2026-04-17 --model-pool "qwen3.5:397b-cloud,gpt-oss:120b-cloud,deepseek-v3.1:671b-cloud,deepseek-v4-pro:cloud" --allow-full-run --require-external-sources --external-search-provider auto --external-search-limit 5 --external-source-min-count 2 --dry-run
```

### Write
```powershell
python -m src.deal_analyzer.training_materials.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --plan-sheet "План недели" --dry-run --strict-preflight
```

## Диагностика
Смотрите `summary.json`:
- `rows_training_candidates`
- `rows_docs_prepared`
- `rows_links_to_write`
- `rows_quarantined`
- `external_sources_used`
- `external_sources_count`
- `external_search_status`
- `source_coverage_passed`
- `block_reason`
