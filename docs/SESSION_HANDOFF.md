# Session Handoff (2026-05-01)

## 1. Что это за проект
Управленческий контур продаж поверх amoCRM + Google Sheets с несколькими слоями аналитики и планирования.

Ключевые листы `РОКС 2026`:
- `Разбор звонков`
- `Дневной контроль`
- `План недели`
- `Недельный свод менеджеров`
- `Свод недели`
- `РОКС ОАП-<месяц 2026>`

## 2. Текущая логика цикла
1. Разбор звонков
2. Дневной контроль
3. Недельный свод менеджеров
4. Свод недели
5. План недели следующего периода
6. Training materials по строкам обучения плана

Особые случаи:
- `2026-03-30..2026-04-03`: bootstrap для week_plan допустим при пустых сигналах.
- Текущая незакрытая неделя: планировать можно, недельные своды закрывать нельзя.

## 3. Role policy (обязательно)
- Илья Бочков = `sales_manager`
  - warm/current pipeline, demo/test/invoice/payment.
  - нельзя массовый cold top-of-funnel как главную задачу.
- Рустам Хомидов = `telemarketer`
  - top-of-funnel (дозвоны, ЛПР, interest) допустим как primary focus.

## 4. Week Plan v2 стандарты
- Daily Task Triad:
  - развитие
  - коммерческий результат
  - контроль
- SMART для задач (включая post-training).
- Duplicate guard и coverage gate должны проходить после preflight.
- CRM-only задачи не могут быть главным коммерческим фокусом.

## 5. Demo standard
Для sales_manager применяется consultative формат:
- educational demo
- guided discovery
- client hands-on
- next-step commitment

## 6. Training materials external sources
Режимы:
- strict: `--require-external-sources`
- warning mode: `--allow-no-external-sources`
- fallback: curated file `docs/training_materials_external_sources.json`

Источники сохраняются в artifacts/debug, но не вставляются в employee-facing Google Doc.

## 7. Операционные команды (основные)
- call_review control day window
- daily_control build/write
- week_plan build/write
- training_materials build/write
- weekly_manager_summary build/write
- week_summary build/write
- cache_manager status/cleanup

См. `README.md` раздел `Known operational commands`.

## 8. Safety: Do not do
- Не запускать real-write без подтверждения и preflight.
- Не менять структуру Google Sheets.
- Не удалять листы.
- Не пушить токены/секреты (`token.json`, `token.backup*.json`).
- Не использовать CRM-only задачи как основной коммерческий план.

## 9. Быстрый checklist перед любым write
1. Прогнать `pytest`.
2. Прогнать dry-run build и проверить block_reason.
3. Убедиться: `rows_in_writer_payload > 0`, `structural_changes_required=false`.
4. Проверить quarantine/debug причины.
5. Только после этого запускать write.

## 10. Важные docs
- `docs/ARCHITECTURE.md`
- `docs/weekly_control_and_base_analysis_spec.md`
- `docs/roks_interpretation.md`
- `docs/training_materials.md`
- `docs/client_list_integration.md`
- `docs/demo_methodology.md`
- `docs/employee_profiles.md`
- `docs/ui_foundation.md`
- `docs/employee_dashboard.md`
