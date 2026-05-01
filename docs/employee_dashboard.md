# Employee Dashboard / Coaching Intelligence

## Назначение
`employee_dashboard` собирает агрегированный профиль сотрудника по evidence-источникам без real-write:
- `Разбор звонков`
- `Дневной контроль`
- `Недельный свод менеджеров`
- `training_materials` payload
- transcript debug (если доступен)

Выход строится с доказательной базой (`evidence_index.json`) и `confidence_score`.

## CLI
```powershell
python -m src.deal_analyzer.employee_dashboard.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-03-30 --period-end 2026-04-30 --employee "Илья Бочков" --dry-run
```

```powershell
python -m src.deal_analyzer.employee_dashboard.cli build --config config/deal_analyzer.call_review.deepseek.realwrite.json --period-start 2026-03-30 --period-end 2026-04-30 --employee "Рустам Хомидов" --dry-run
```

## Артефакты
Run dir: `workspace/employee_dashboard/<run_id>_<employee_slug>/`

- `employee_dashboard_summary.json`
- `employee_dashboard_summary.md`
- `speech_modules_debug.json`
- `objection_patterns_debug.json`
- `evidence_index.json`
- `summary.json`

## Confidence и evidence
- `source_coverage_passed=true`, если есть минимум 2 непустых источника и минимум 5 evidence-строк.
- `confidence_score` считается детерминированно по объему evidence, покрытию источников, речевым модулям и возражениям.
- Если evidence нет, `confidence_score=0`.

## Интеграционные хуки
`summary.json` содержит пути, которые могут читать следующие контуры:
- week_plan (контекст сотрудника)
- training_materials (повторяющиеся зоны роста)
- UI (готовый markdown-summary)
