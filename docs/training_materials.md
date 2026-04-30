# Training Materials (LLM-First)

## Scope

`training_materials` генерирует документы обучения и задания после обучения по строкам из листа `План недели`, где `activity_type=обучение`.

Ключевой принцип:
- источники (включая интернет) используются для grounding и контроля качества;
- в Google Doc для сотрудника **не вставляется** раздел `Использованные источники`;
- внешние URL в текст обучения/задания **не вставляются**.

## Model Policy

Текущая production-политика проекта:
- `call_review` / `daily_control`:
  - main: `qwen3.5:397b-cloud`
  - fallback: `deepseek-v3.1:671b-cloud`

Для `training_materials` рекомендуется model-pool:
1. `qwen3.5:397b-cloud`
2. `gpt-oss:120b-cloud`
3. `deepseek-v3.1:671b-cloud`
4. `deepseek-v4-pro:cloud` (premium retry для проблемных строк)

`deepseek-v4-pro:cloud` не должен быть массовым default для всего потока, а используется как усиленный fallback/retry.

## External Sources: How It Works

Для каждой training-строки собираются 4 группы источников:
1. `style`:
   - `docs/мой паттерн общения.txt`
   - `docs/style_sources/**`
2. `speech`:
   - внутренние спичи/скрипты (по файлам и содержимому в `docs/**`)
3. `product`:
   - внутренний продуктовый контекст (`istock.link`, `istock.info`, PLM и др.)
4. `external`:
   - внешний интернет-поиск методик продаж/переговоров.

Провайдер внешнего поиска:
- `auto` (цепочка: `http_json` -> `duckduckgo_html` -> `manual_curated_urls`)
- `http_json`
- `duckduckgo_html`
- `manual_curated_urls`
- `disabled`

Статусы внешнего поиска:
- `ok`
- `no_results`
- `unavailable`
- `disabled`
- `provider_error`

## Почему источники в артефактах, а не в Google Doc

Это сделано специально:
- документ сотруднику должен быть практичным учебным материалом, а не отчетом/курсовой;
- внешние ссылки и список источников могут засорять и ухудшать читаемость;
- проверяемость обеспечивается через artifacts run-dir.

То есть:
- в текст обучения источники не вставляем;
- в artifacts сохраняем полный trace по источникам.

## Quality and Lint (Only for training_materials)

Ослабление “цензуры” применяется только к `training_materials`:
- `foreign_words_detected` по бизнес-терминам -> warning, не blocker;
- URL/названия CRM/методик/продуктов не валят документ.

Блокеры остаются для:
- битой кодировки/mojibake/CJK-мусора;
- отсутствия структуры;
- недостатка речевых модулей/чек-листа;
- слишком короткого текста;
- вставки раздела `Использованные источники` в user-facing doc;
- вставки внешних URL в user-facing doc.

## CLI Flags

### Discover
```powershell
python -m src.deal_analyzer.training_materials.cli discover --config <CONFIG> --plan-sheet "План недели"
```

### Build (dry-run)
```powershell
python -m src.deal_analyzer.training_materials.cli build `
  --config <CONFIG> `
  --plan-sheet "План недели" `
  --daily-sheet "Дневной контроль" `
  --call-review-sheet "Разбор звонков" `
  --week-start YYYY-MM-DD `
  --week-end YYYY-MM-DD `
  --model-pool "qwen3.5:397b-cloud,gpt-oss:120b-cloud,deepseek-v3.1:671b-cloud,deepseek-v4-pro:cloud" `
  --require-external-sources `
  --external-search-provider auto `
  --external-search-limit 5 `
  --external-source-min-count 2 `
  --allow-full-run `
  --dry-run
```

Новые важные флаги build:
- `--model-pool`
- `--require-external-sources` (default: true)
- `--allow-no-external-sources` (default: false)
- `--external-search-provider`
- `--external-search-limit`
- `--external-source-min-count`
- `--allow-full-run`

Ограничения/контроль выполнения:
- `--limit`, `--offset`
- `--max-runtime-minutes`, `--max-llm-calls`
- `--main-timeout`, `--fallback-timeout`
- `--resume-run-dir`, `--retry-failed-from-run-dir`, `--resume`

### Write
```powershell
python -m src.deal_analyzer.training_materials.cli write --config <CONFIG> --run-dir <RUN_DIR> --plan-sheet "План недели" --dry-run --strict-preflight
```

Real-write только по явному `--write`.

## External Source Requirements

По умолчанию build требует внешние источники:
- `require_external_sources=true`
- минимум `external_source_min_count=2` на строку.

Если внешний поиск недоступен:
- без исключения build блокируется:
  - `block_reason=external_sources_unavailable`
- осознанный обход только через:
  - `--allow-no-external-sources`

## Как читать source_coverage/debug

### 1) `summary.json`
Смотреть:
- `external_search_status`
- `external_sources_used`
- `external_sources_count`
- `source_coverage_passed`
- `source_coverage_failed_rows`
- `block_reason`
- `action_required`

### 2) `training_materials_source_coverage.json`
Сводка по run:
- использованы ли `style/speech/product/external` источники;
- сколько внешних источников найдено;
- выполнен ли coverage gate.

### 3) `training_materials_external_sources_debug.json`
Детализация по каждой строке:
- `row_number`, `recipient`, `plan_date`
- `external_search_status`
- `external_sources_count`
- `external_source_titles`
- `external_source_urls`
- `external_source_fetch_errors`
- `source_coverage_passed`
- `source_coverage_fail_reasons`

### 4) `training_materials_quarantine.json`
Какие строки заблокированы и почему (включая source coverage и quality).

## Practical Diagnostics

Если видите `external_sources_unavailable`:
1. проверьте интернет/доступ к провайдеру;
2. проверьте `external_search_provider`;
3. для `http_json` проверьте endpoint/key;
4. для `manual_curated_urls` задайте curated URLs;
5. если надо временно продолжить без интернета, запускайте только осознанно:
   - `--allow-no-external-sources`.

