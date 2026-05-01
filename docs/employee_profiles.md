# Employee Profiles

## Назначение
`employee_profiles` добавляет детерминированную персонализацию коучинга поверх аналитики для:
- `daily_control`
- `week_plan`
- `training_materials`
- `weekly_manager_summary`
- `week_summary`

## Базовый registry
Пример конфига:

```json
{
  "employee_profiles": {
    "Рустам Хомидов": {
      "communication_style": "direct_accountability",
      "motivators": ["responsibility", "discipline", "visible_progress"],
      "avoid": ["soft_generic_advice"]
    },
    "Илья Бочков": {
      "communication_style": "expert_to_expert",
      "motivators": ["commercial_effect", "autonomy", "professional_mastery"],
      "avoid": ["tool_for_tool_sake", "crm_moralizing"]
    }
  }
}
```

## Стилевые правила
- `direct_accountability`: прямой управленческий тон, фокус на ответственности и дисциплине.
- `expert_to_expert`: профессиональный тон, фокус на коммерческом результате и мастерстве.

## Safety safeguards
- Запрещены оскорбления и унижение сотрудника.
- Для Рустама: жестко, но по делу и без токсичности.
- Для Ильи: профессионально, без «морали про CRM» и без «tool for tool sake».

## Future learning markers (deterministic)
Собираются маркеры:
- repeated growth zones
- repeated strong sides
- repeated objections handled well/badly
- preferred behavior pattern under pressure
- coaching response style

## Артефакты
- `employee_profile_context_debug.json`
- `employee_behavior_markers.json`

## Связь с role policy
Profile-тон применяется поверх role-based задач:
- Илья (`sales_manager`) не получает mass cold primary focus.
- Рустам (`telemarketer`) может получать top-of-funnel primary focus.
