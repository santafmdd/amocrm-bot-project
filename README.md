# amoCRM Bot Project

Проект автоматизирует управленческий контур продаж в Google Sheets на базе amoCRM данных.

## Что это за контур
Основные листы файла `РОКС 2026`:
- `Разбор звонков`: построчный качественный разбор кейсов звонков/контактов.
- `Дневной контроль`: ежедневный управленческий срез по менеджеру и дню.
- `План недели`: план действий руководителя по менеджерам и датам.
- `Недельный свод менеджеров`: итог недели по каждому менеджеру с plan-fact.
- `Свод недели`: итог недели по отделу.
- `РОКС ОАП-<месяц 2026>`: план/факт KPI и недельные блоки воронки.

## Как слои связаны
Нормальный цикл:
1. `Разбор звонков` -> 2. `Дневной контроль` -> 3. `Недельный свод менеджеров` -> 4. `Свод недели` -> 5. `План недели (следующая неделя)`.

Bootstrap-исключение:
- Для первой недели `2026-03-30..2026-04-03` допускается bootstrap для `План недели`, потому что до этой даты еще нет полноценного history-сигнала из daily/call-review.

Текущая (незакрытая) неделя:
- Планировать можно (`План недели` + `training_materials`).
- Сводить недельные итоги (`Недельный свод менеджеров` / `Свод недели`) до закрытия недели нельзя.

## Role policy
- `Илья Бочков` = `sales_manager`.
  - Фокус: теплый/текущий пайплайн (`interest->demo->test->invoice->payment`, renewals/reactivation).
  - Нельзя как главный фокус дня: массовый верх воронки, холодный обзвон, «20 звонков по базе», «наборы/дозвоны».
- `Рустам Хомидов` = `telemarketer`.
  - Фокус: верх воронки (`дозвоны`, `ЛПР`, `есть интерес`, назначение встреч).
  - Верх воронки для него допустим как основной контур.

## Week plan v2 стандарты
- Daily Task Triad в каждой строке дня:
  - `1. Развитие`
  - `2. Коммерческий результат`
  - `3. Контроль`
- SMART для задач (особенно post-training): конкретика, метрика, срок, привязка к воронке/сделкам, критерий проверки.
- CRM-only задачи не могут быть главным коммерческим фокусом дня.

## Demo methodology
Для `sales_manager` применяется стандарт:
- educational demo
- guided discovery
- client hands-on
- problem-based walkthrough
- next-step commitment

Демо не должно быть агрессивной «презентацией всех функций».

## Training materials external sources
`training_materials build` поддерживает:
- `--require-external-sources` (по умолчанию strict)
- `--allow-no-external-sources` (ослабление с warning)
- auto provider chain + curated fallback (`docs/training_materials_external_sources.json`).

Источники используются для grounding и диагностики, но не вставляются в user-facing Google Doc.

## Long-running jobs: progress + cache
Реализованы:
- `progress.json`, `progress.log`, `heartbeat.json` в run-dir долгих процессов.
- `cache_manager`:
  - status
  - cleanup dry-run
  - cleanup delete (только разрешенные cache/media директории).

## UI foundation
Есть минимальный FastAPI backend для запуска сухих/боевых job с safety-контрактом:
- real_write по умолчанию выключен;
- real_write требует confirmation token;
- структурные операции блокируются политикой UI.

Подробно: `docs/ui_foundation.md`.

## Known operational commands

### 1) call_review control_day_window
```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.call_review.deepseek.realwrite.json analyze-period --period-mode control_day_window --control-date 2026-04-29 --business-cutoff 15:00 --business-timezone Europe/Moscow --discussion-limit 120 --limit 120
```

### 2) daily_control build/write
```powershell
python -m src.deal_analyzer.daily_control.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-29 --period-end 2026-04-29 --source-sheet "Разбор звонков" --daily-sheet "Дневной контроль" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
python -m src.deal_analyzer.daily_control.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --daily-sheet "Дневной контроль" --dry-run --strict-preflight --allow-partial-write --quarantine-unrepaired
```

### 3) week_plan build/write
```powershell
python -m src.deal_analyzer.week_plan.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --signal-start 2026-04-06 --signal-end 2026-04-10 --plan-week-start 2026-04-13 --plan-week-end 2026-04-17 --daily-sheet "Дневной контроль" --target-sheet "План недели" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
python -m src.deal_analyzer.week_plan.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --target-sheet "План недели" --dry-run --strict-preflight --allow-partial-write --quarantine-unrepaired
```

### 4) training_materials build/write
```powershell
python -m src.deal_analyzer.training_materials.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --plan-sheet "План недели" --daily-sheet "Дневной контроль" --call-review-sheet "Разбор звонков" --week-start 2026-04-13 --week-end 2026-04-17 --model-pool "qwen3.5:397b-cloud,gpt-oss:120b-cloud,deepseek-v3.1:671b-cloud,deepseek-v4-pro:cloud" --allow-full-run --require-external-sources --dry-run
python -m src.deal_analyzer.training_materials.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --plan-sheet "План недели" --dry-run --strict-preflight
```

### 5) weekly_manager_summary build/write
```powershell
python -m src.deal_analyzer.weekly_manager_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Дневной контроль" --plan-sheet "План недели" --target-sheet "Недельный свод менеджеров" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
python -m src.deal_analyzer.weekly_manager_summary.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --target-sheet "Недельный свод менеджеров" --dry-run --strict-preflight
```

### 6) week_summary build/write
```powershell
python -m src.deal_analyzer.week_summary.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-04-27 --period-end 2026-05-03 --daily-sheet "Дневной контроль" --plan-sheet "План недели" --manager-summary-sheet "Недельный свод менеджеров" --target-sheet "Свод недели" --main-model qwen3.5:397b-cloud --fallback-model deepseek-v3.1:671b-cloud --dry-run
python -m src.deal_analyzer.week_summary.cli write --config config/deal_analyzer.call_review.deepseek.realwrite.json --run-dir <RUN_DIR> --target-sheet "Свод недели" --dry-run --strict-preflight
```

### 7) cache cleanup
```powershell
python -m src.deal_analyzer.cache_manager status --config config/deal_analyzer.call_review.deepseek.realwrite.json
python -m src.deal_analyzer.cache_manager cleanup --config config/deal_analyzer.call_review.deepseek.realwrite.json --older-than-days 14 --max-size-gb 20 --dry-run
```

## Do not do
- Не запускать real-write без явного подтверждения и preflight.
- Не менять структуру Google Sheets (headers/tabs/validation) без отдельной команды.
- Не удалять листы.
- Не пушить токены/секреты.
- Не добавлять в git `token.backup*.json`.
- Не использовать CRM-only задачи как главный коммерческий план.

## Docs map
- `docs/ARCHITECTURE.md`
- `docs/SESSION_HANDOFF.md`
- `docs/weekly_control_and_base_analysis_spec.md`
- `docs/roks_interpretation.md`
- `docs/training_materials.md`
- `docs/client_list_integration.md`
- `docs/demo_methodology.md`
- `docs/employee_profiles.md`
- `docs/ui_foundation.md`
- `docs/employee_dashboard.md`
