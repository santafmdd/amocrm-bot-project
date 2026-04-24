# Deal Analyzer MVP (rules + hybrid + Ollama)

## Scope
Deal analyzer reads collector output and builds local analytical result.

Current constraints:
- no writes to Google Sheets;
- no UI;
- no weekly/refusals/analytics writer changes;
- exports only to `workspace/deal_analyzer`.

## Backends
`analyzer_backend` in config:
- `rules` - deterministic rule-based analyzer (default).
- `hybrid` - rules-first analysis + optional short LLM layer.
- `ollama` - local LLM analysis through Ollama chat API.

Rules backend remains available as stable fallback.

## Hybrid backend (safe overlay)
`hybrid` never replaces base rules scoring.

Flow:
1. Build standard rules analysis (mandatory).
2. If Ollama is available, request only a small structured JSON overlay:
   - `loss_reason_short`
   - `manager_insight_short`
   - `coaching_hint_short`
3. On timeout / invalid JSON / model error:
   - run does not crash;
   - base rules result is kept;
   - artifact marks fallback/error with `llm_fallback=true` and `llm_error=true`.

This keeps period runs stable while still adding optional short insights when LLM response is valid.

### Narrow LLM Overlay (current practical mode)
LLM используется как узкий semantic-overlay поверх rules, не как замена deterministic анализа.

Rules остаются источником истины для:
- `score_0_100`
- `risk_flags`
- `data_quality_flags`
- `owner_ambiguity_flag`
- `analysis_confidence`
- queue category (`why_in_queue`)

LLM overlay заполняет только короткие поля:
- `product_hypothesis_llm`
- `loss_reason_short`
- `manager_insight_short`
- `coaching_hint_short`
- `reanimation_reason_short_llm`

Если LLM недоступна/ошибается, run не падает и остается rules-only fallback.

## Ollama Reliability: Preflight + Repair + Fallback
For `analyze-period` with `analyzer_backend=ollama`:
- CLI runs a short preflight probe (`/api/chat`, small JSON payload).
- If preflight fails, run does not crash: whole period is processed through rules fallback.
- For each deal, parser tries strict JSON first, then safe repair:
  - trims extra text before/after JSON,
  - handles ```json ... ``` fences,
  - extracts balanced JSON object if model adds noise.
- If repair succeeds, deal is counted as `llm_success_repaired`.
- If retry still fails, only that deal falls back to rules (`rules_fallback`), batch continues.

## External Enrichment Sources (read-only)
Analyzer can enrich deals from two external Google Sheets sources:
- client list
- appointment list

Config switches:
- `client_list_enrich_enabled`
- `appointment_list_enrich_enabled`
- `client_list_source_url`
- `appointment_list_source_url`
- `client_list_sheet_name`
- `appointment_list_sheet_name`
- `matching_strategy`
- `fields_mapping`
- `operator_outputs_enabled`

### Matching priority
For each source, matching is applied in this priority:
1. `deal_id` / explicit external id
2. phone
3. email
4. company + contact name
5. company only

Per-deal enrichment fields in export:
- `enrichment_match_status`
- `enrichment_match_source`
- `enrichment_confidence`
- `matched_client_list_row_id`
- `matched_appointment_row_id`

Additional pulled fields:
- from client list: test started/completed/status/comments
- from appointment list: appointment date, assigned by, conducted by, meeting status, transfer/cancel flag

## Operator Outputs
When `operator_outputs_enabled=true`, each deal includes:
- `manager_summary`
- `employee_coaching`
- `employee_fix_tasks` (3-7 tasks)

Tone requirements in generated text:
- business/human internal style
- no mentions of AI/LLM
- short actionable wording

## Period metadata/output artifacts
Period metadata includes:
- `llm_success`
- `llm_success_repaired`
- `llm_fallback`
- `llm_error`
- `backend_requested`
- `backend_effective_summary`

Per-deal backend fields:
- `analysis_backend_requested`
- `analysis_backend_used`
- `llm_repair_applied`
- `llm_error`
- `llm_fallback`
- `loss_reason_short`
- `manager_insight_short`
- `coaching_hint_short`

## Period Modes
Supported `period_mode` values:
- `smart_manager_default`
- `current_week_to_date`
- `previous_calendar_week`
- `previous_workweek`
- `custom_range`

Semantics:
- `current_week_to_date`: Monday of current week .. run date.
- `previous_calendar_week`: previous week Monday..Sunday.
- `previous_workweek`: previous week Monday..Friday.
- `custom_range`: explicit `date_from` + `date_to`.
- `smart_manager_default`:
  - Saturday/Sunday run -> `current_week_to_date`
  - Monday-Friday run -> `previous_workweek`

## Public Export Visibility
Config controls public metadata:
- `hide_executed_at_from_public_exports`
- `executed_at_visibility` (`internal_only` | `public`)
- `period_label_mode` (`period_only` | `period_and_as_of`)

Public exports (json/md/csv) always include:
- `period_start`
- `period_end`
- `public_period_label`
- `as_of_date`

`executed_at` is hidden from public exports by default.

## CLI
Analyze one deal:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-deal --input workspace/amocrm_collector/deal_31913530_latest.json
```

Analyze period:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --period-mode previous_workweek --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json
```

Опционально можно ограничить размер батча:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --limit 20
```

Фильтры очереди обсуждения (применяются после построения per-deal analysis records):

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-period --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --limit 20 --owner-contains "Илья" --product-contains "ИНФО" --status-contains "В работе" --exclude-low-confidence --discussion-limit 10
```

### Period artifacts
`analyze-period` теперь дополнительно создает batch run-папку:
- `workspace/deal_analyzer/period_runs/<run_timestamp>/deals/deal_<id>.json` — per-deal snapshot+analysis artifacts;
- `workspace/deal_analyzer/period_runs/<run_timestamp>/summary.json` — агрегированный итог запуска.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/summary.md` — человекочитаемый итог run.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/top_risks.json` — быстрый риск-лист по сделкам.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/manager_brief.md` — короткий управленческий бриф для быстрого чтения.
- `workspace/deal_analyzer/period_runs/<run_timestamp>/meeting_queue.json` — queue для обсуждения на встрече (с фильтрами/приоритетом).
- `workspace/deal_analyzer/period_runs/<run_timestamp>/meeting_queue.md` — человекочитаемый meeting queue.

Категории queue (`why_in_queue`):
- `active_risk` — живой риск по активной сделке.
- `won_handoff_check` — проверка передачи выигранной сделки.
- `low_confidence_needs_manual_check` — ручная проверка из-за низкой надежности интерпретации/owner ambiguity.
- `qualified_loss_for_pattern_review` — паттерн осознанной потери для разбора.
- `closed_lost_cleanup_review` — закрытая потеря без qualified-loss, требующая closeout/cleanup разбора.

Deterministic порядок queue:
1. `active_risk`
2. `won_handoff_check`
3. `low_confidence_needs_manual_check`
4. `qualified_loss_for_pattern_review`
5. `closed_lost_cleanup_review`

Человекочитаемая версия причины для операционного экспорта:
- `why_in_queue_human` — текст для таблицы/разбора на встрече;
- `why_in_queue` — технический код категории (оставлен для диагностики и фильтров).

Reanimation layer для closed-lost (отдельно от queue category):
- `reanimation_potential`: `none|low|medium|high`
- `reanimation_reason_short`
- `reanimation_next_step`
- `reanimation_risk_note`

Важно:
- `why_in_queue` отвечает, почему кейс попал в очередь разбора.
- reanimation-поля отвечают, есть ли смысл пытаться вернуть closed-lost кейс.
- reanimation не меняет queue category напрямую.

### Product Hypothesis Layer
`product_name` (CRM факт) не заменяется и не перетирается.

Отдельно в per-deal/queue артефактах добавляется гипотеза продукта:
- `product_hypothesis`: `info|link|mixed|unknown`
- `product_hypothesis_confidence`: `low|medium|high`
- `product_hypothesis_sources`: список источников сигналов
- `product_hypothesis_reason_short`: короткое объяснение

Разница:
- `CRM product` = то, что явно заполнено в CRM.
- `product hypothesis` = best-effort интерпретация по совокупности сигналов (звонки/notes/tasks/tags/raw values/status).
- `hypothesis confidence` = уверенность в гипотезе с понижением при low-confidence/owner-ambiguity кейсах.

## Daily Control: LLM-authored Table Text
Для `Дневной контроль` текстовые ячейки теперь формируются через LLM (при `analyzer_backend=hybrid|ollama`), а rules-слой подает только факты и guardrails:
- day packaging, allowlist менеджеров, role-aware ограничения;
- score/criticality/confidence как read-only вход;
- call-first контекст (если есть звонки/транскрипт).

LLM генерирует поля:
- `Ключевой вывод`
- `Сильные стороны`
- `Зоны роста`
- `Почему это важно`
- `Что закрепить`
- `Что исправить`
- `Что донес сотруднику`
- `Ожидаемый эффект - количество`
- `Ожидаемый эффект - качество`

Жесткое правило записи:
- если main LLM недоступна и fallback LLM тоже недоступна, real write в Google Sheet запрещен;
- run продолжает работать в `dry_run`, а причина фиксируется в `summary.json` (`daily_control_writer.error=write_forced_dry_run_no_live_llm`);
- в боевой лист пишутся только строки с `llm_text_ready=true` (без rules-only J-Q текста).

Политика эффектов:
- `Ожидаемый эффект - количество` — только абсолютные результаты (без процентов);
- `Ожидаемый эффект - качество` — аккуратная гипотеза про качество этапов/конверсии, без завышенных обещаний.

Style source: `docs/мой паттерн общения.txt` (используется как ориентир тона; если недоступен — включается безопасный fallback).

### Reference Stack (обязательный порядок)
Для смысловых колонок (`Ключевой вывод`, `Сильные стороны`, `Зоны роста`, `Почему это важно`, `Что закрепить`, `Что исправить`, `Что донес сотруднику`, `Ожидаемый эффект-*`) prompt собирается с обязательным стеком источников:
1. Внутренние референсы (sales scripts + локальные reference docs).
2. Продуктовые URL из `product_reference_urls`.
3. Внешний retrieval (опционально, только если включен).

Диагностика по реально подмешанным источникам пишется в:
- `period_runs/<run_id>/daily_reference_stack_debug.json`
- row-level debug в `daily_control_sheet_payload.json` (`reference_sources_*`, `external_retrieval_*`).

### Optional External Retrieval Layer
Локальная Ollama не ходит в интернет сама. Для внешних рекомендаций есть отдельный optional adapter-слой:
- `external_retrieval_enabled` (`false` по умолчанию)
- `external_retrieval_adapter` (`none|http_json`)
- `external_retrieval_endpoint`
- `external_retrieval_timeout_seconds`
- `external_retrieval_top_k`
- `external_retrieval_api_key`
- `external_retrieval_query_prefix`

Если retrieval выключен/не настроен/ошибся, pipeline не падает: работает только на внутренних источниках + product URLs.

### Style Mode
`daily_style_mode`:
- `mild` (по умолчанию): живой рабочий текст без грубости.
- `work_rude`: допускается умеренно жесткая лексика (без оскорблений и буллинга).

Style layer меняет только формулировку, не факты и не структуру блоков.

### Daily candidate pool and ranking
Daily row собирается не из первых сделок периода, а через `candidate-pool -> ranking -> top selection`:
- приоритет 1: call-rich и информативные сделки (usable transcript / сигнал из звонка);
- приоритет 2: rich CRM-context сделки (notes/tasks/comments/tags);
- приоритет 3: thin fallback только если иначе пакет дня не собрать честно.

Debug-only поля в `daily_control_sheet_payload.json`:
- `transcript_usability_score`
- `evidence_richness_score`
- `funnel_relevance_score`
- `daily_selection_rank`
- `daily_selection_reason`

### Conversation/Discipline pool split (pre-limit)
Перед применением `--limit` analyzer строит два отдельных пула по звонковой мете:
- `conversation_pool` (переговорные кейсы),
- `discipline_pool` (дисциплина звонков и dead-redial паттерны).

Артефакты run-папки:
- `conversation_pool.json` / `conversation_pool.md`
- `discipline_pool.json` / `discipline_pool.md`
- `discipline_report.json` / `discipline_report.md`

Для каждой сделки фиксируются:
- `pool_type`
- `pool_reason`
- `pool_priority_score`
- `call_case_type` (`lpr_conversation`, `secretary_case`, `supplier_inbound`, `warm_inbound`, `redial_discipline`, `autoanswer_noise`, `unknown`)

`--limit` после этого шага применяется к shortlist из `conversation_pool`, а не к сырому списку period deals.

В STT-поток идут только shortlist-calls из `conversation_pool`:
- 1 основной разговор + до 1-2 связанных follow-up звонков;
- short/no-answer/autoanswer шум отсекается;
- `discipline_pool` не отправляется в основной STT.

### Daily package selection order (call-first)
Daily package теперь выбирается в явном порядке:
1. usable `conversation_pool` кейсы (`lpr/secretary/supplier/warm`);
2. `discipline_pool` только если нет нормальных negotiation-кейсов;
3. CRM-thin filler (без usable звонка и без meaningful discipline-pattern) блокируется.

Новые debug-поля в daily row:
- `daily_primary_source` (`conversation_pool|discipline_pool`)
- `daily_case_type`
- `daily_selection_reason_v2`
- `excluded_crm_only_cases_count`

Новый run-артефакт:
- `daily_selection_debug.json`

Новые summary-поля:
- `daily_rows_from_conversation_pool`
- `daily_rows_from_discipline_pool`
- `daily_rows_skipped_crm_only`
- `daily_rows_with_real_transcript`
- `daily_rows_with_only_discipline_signals`

### Role-aware scope (телемаркетолог vs менеджер по продажам)
Система применяет role-scope не косметически, а как отдельный фильтр для:
- `strong_sides`
- `growth_zones`
- `fix_action`
- `coaching`
- `why_important`
- `expected effect`

Матрица:
- `телемаркетолог` (allowed):
  - проход секретаря, выход на ЛПР, квалификация, назначение встречи,
  - дисциплина звонков, покрытие номеров, недозвоны/автоответчики/повторы наборов.
- `телемаркетолог` (blocked by default):
  - презентация, демонстрация, бриф, тест, КП/счет/оплата, дожим после демо.
- `менеджер по продажам` (allowed):
  - демонстрация, бриф, тест, следующий шаг после встречи, счет/КП/оплата,
  - зависание после теплого этапа, quality follow-up после демо/теста.

Role conflict behavior:
- если для телемаркетолога есть сильный явный звонковый сигнал, что кейс реально ушел в warm/demo/test,
  блок по части warm-тем ослабляется (`role_scope_conflict_flag=true`);
- иначе warm-темы вырезаются из user-facing текста.

Debug поля в daily row:
- `role_scope_applied`
- `role_blocked_topics`
- `role_allowed_topics`
- `role_scope_conflict_flag`

### Conversation insight vs discipline insight
- `conversation insight`:
  - разбор содержательных разговоров (LPR/secretary/supplier/warm inbound),
  - используется для переговорного анализа и транскрибации.
- `discipline insight`:
  - отдельный контур по дисциплине звонков (повторы, недозвоны, автоответчики, покрытие номеров),
  - не подменяет переговорный анализ,
  - оформляется в `discipline_report.*`.

`discipline_report.json` по каждой сделке discipline_pool включает:
- `unique_phone_count`
- `attempts_total`
- `attempts_per_phone`
- `phones_over_2_attempts`
- `repeated_dead_redial_count`
- `same_time_redial_pattern_flag`
- `numbers_not_fully_covered_flag`
- `short_call_cluster_flag`
- `autoanswer_cluster_flag`
- `discipline_summary_short`
- `discipline_risk_level`

В `summary.md` и `manager_brief.md` секции разделены явно:
- `negotiation_analysis`
- `discipline_analysis`

### ROKS hook for ranking/effect forecast
Добавлен подготовительный hook под stage-priority weighting из ROKS:
- если `roks_stage_priority_weights` доступны, ranking учитывает их;
- если нет — используется нейтральный fallback (`stage_priority_weight_source=neutral_fallback`).

Для `Ожидаемый эффект`:
- количество считается через абсолютный каскад по этапам (без процентов в колонке количества);
- качество описывает влияние на смежные этапы как аккуратную гипотезу;
- если ROKS-метрики недоступны, используется консервативный fallback и это явно логируется.

`summary.json` включает:
- `run_timestamp`
- `backend_requested`
- `analysis_backend_used` (+ counts по backend used)
- `total_deals_seen`
- `total_deals_analyzed`
- `deals_failed`
- `artifact_paths`
- `score_aggregates` (`min/max/avg`)
- `risk_flags_counts`
- `transcript_runtime_diagnostics`:
  - `deals_with_any_call_evidence`
  - `deals_with_audio_path`
  - `deals_with_transcript_text`
  - `deals_with_transcript_excerpt`
  - `deals_with_nonempty_call_signal_summary`
  - `deals_with_transcription_error`
  - `transcript_layer_effective`

Как понять, что транскрибация реально участвовала в run:
- `transcript_layer_effective=true`, и/или
- в `summary.md`/`manager_brief.md` в секции `Проверка транскрибации` значение “Сделок реально дали смысл в анализе” больше нуля.

`summary.md` — это операторский markdown-срез с ключевыми секциями:
- run info;
- score aggregates;
- top risk flags;
- top 10 risky deals;
- top 10 highest score deals;
- пометка `[warnings]` для сделок со snapshot warnings.

`top_risks.json` — массив по сделкам для быстрого разбора риска/приоритезации.

`manager_brief.md` — управленческий компактный отчет:
- объем (просмотрено/проанализировано/упало),
- 5 основных риск-паттернов,
- отдельный блок `Qualified loss / market mismatch`,
- 5 сделок внимания,
- 5 сделок с лучшим потенциалом,
- короткий блок "что делать дальше".

## Weekly management layer
Новый CLI-сценарий:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-weekly --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --limit 10
```

Создает в `workspace/deal_analyzer/weekly_runs/<timestamp>/`:
- `rustam_weekly.md` — недельная сводка по Рустаму (ранние этапы: ЛПР/квалификация/встреча);
- `ilya_weekly.md` — недельная сводка по Илье (демо/тест/follow-up/счет/оплата);
- `weekly_meeting_brief.md` — общий brief руководителя на weekly встречу;
- `next_week_plan.md` — заготовка плана Monday-Friday;
- `summary.json` — техническая сводка запуска (counts, backend, llm metrics, output paths).

Guardrails сохраняются:
- low-confidence / owner-ambiguity кейсы не используются для жестких персональных выводов;
- quality limits явно выносятся в weekly markdown-артефакты.

### Rules quality slice (latest)
- Rules scoring теперь учитывает не только demo/brief, но и контекст из CRM: notes/tasks/tags, контактные и company-данные, long-call сигналы.
- Сценарии `reasoned loss` (осознанный отказ/market mismatch) помечаются отдельно как `qualified_loss:*`, а не смешиваются с пустыми hygiene-кейсами.
- В period markdown-отчетах `qualified_loss` выводится отдельной секцией.

### Policy-aware recommendations
- `qualified_loss` (осознанный отказ/market mismatch): рекомендации смещаются в фиксацию причины, сегментный вывод и снятие лишнего follow-up давления.
- `evidence_context` gap: приоритет — заполнение CRM-контекста (notes, pain, business task, evidence).
- `process_hygiene` gap (без qualified_loss): сохраняются классические follow-up/next-step/probability рекомендации.
- `closed_lost + evidence_context gap`: рекомендации про восстановление причины потери и корректную классификацию closeout, без дефолтного прогрева/демо.
- `won`: безопасный минимум по post-sale/handoff (без агрессивных pipeline pressure рекомендаций).
- Для all-loss batch manager artifacts рендерятся безопасно: блок потенциала не вводит в заблуждение и показывает fallback по закрытым кейсам.

Это технический batch slice для analyzer и snapshot pipeline, не weekly layer и не writer layer.

## Vertical Slice: Snapshot -> Analysis -> JSON
Минимальный сценарий для одной сделки/снапшота:

1. Получить prepared snapshot (например, из `build-call-snapshot`) **или** взять collector input и указать `--deal-id`.
2. Запустить:

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-snapshot --input workspace/deal_analyzer/call_snapshot_deal_latest.json
```

или

```powershell
python -m src.deal_analyzer.cli --config config/deal_analyzer.local.json analyze-snapshot --input workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json --deal-id 31913530
```

Результат:
- JSON artifact в `workspace/deal_analyzer/analyze_snapshot_<timestamp>.json`;
- latest copy: `workspace/deal_analyzer/analyze_snapshot_latest.json` (если не задан `--no-latest`);
- в логе фиксируется `backend_requested` и фактический `analysis_backend_used`.

Покрыто сейчас:
- стабильный snapshot (включая partial warnings);
- запуск текущего backend (`rules`/`ollama`) поверх `snapshot.crm`;
- сохранение итогового JSON.

Пока не покрыто:
- запись результата в Google Sheets;
- UI/оркестрация внешних запусков.

## Notes
- Enrichment is read-only (no write-back to Google Sheets).
- Weekly/refusals/analytics writer flows are untouched.

## Data Quality / Owner Ambiguity Guardrails
- Analyzer now marks interpretation reliability with:
  - `data_quality_flags`
  - `owner_ambiguity_flag`
  - `crm_hygiene_confidence` (`high|medium|low`)
  - `analysis_confidence` (`high|medium|low`)
- Conservative owner ambiguity heuristic is used when CRM owner differs from enriched meeting actors (`enriched_conducted_by` / `enriched_assigned_by`) or attribution is limited.
- For low-confidence deals, manager/employee outputs switch to caution-first wording:
  - no accusatory assumptions about manager inactivity;
  - focus on fact restoration, owner attribution check, and CRM cleanup.
- Artifacts expose these markers in per-deal JSON and period markdown/csv outputs.

### Weekly Meeting readability (manager artifacts)
- `manager_brief.md` and `summary.md` now include explicit short sections for weekly review:
  - **что просело сильнее всего** (top drop signals from risk patterns),
  - **что можно исправить за 1 неделю** (short operational fixes),
  - **что нельзя интерпретировать уверенно из-за качества CRM** (low-confidence / owner ambiguity / closed-lost noise).
- These sections are advisory and avoid accusatory language when confidence is low.

## Minimal transcript-aware analysis layer
- Добавлен практичный MVP-слой сигналов из транскрипта звонка (keyword/phrase heuristics, без тяжелого NLP).
- Новые поля в analysis/per-deal:
  - `transcript_available`
  - `transcript_text_excerpt`
  - `call_signal_product_info`
  - `call_signal_product_link`
  - `call_signal_demo_discussed`
  - `call_signal_test_discussed`
  - `call_signal_budget_discussed`
  - `call_signal_followup_discussed`
  - `call_signal_objection_price`
  - `call_signal_objection_no_need`
  - `call_signal_objection_not_target`
  - `call_signal_next_step_present`
  - `call_signal_decision_maker_reached`
- Если транскрипта нет, run не падает: `transcript_available=false`, сигналы пустые.
- В markdown-артефактах добавлен call-aware срез:
  - `summary.md` / `manager_brief.md` / `weekly_meeting_brief.md` — агрегаты по call-сигналам;
  - `meeting_queue.md` — короткая строка “По звонку видно: ...” для кейсов с транскриптом.
- Guardrails сохраняются: call-сигналы не заменяют deterministic score/risk/queue слой и не отменяют data-quality/owner-ambiguity ограничения.

## Local speech-to-text via faster-whisper
- Добавлен локальный backend `transcription_backend=faster_whisper`.
- Дефолтная модель: `large-v3-turbo` (поддержан alias `whisper-large-v3-turbo`, который автоматически нормализуется).
- Новые настройки в конфиге:
  - `whisper_model_name`
  - `whisper_device` (`auto|cuda|cpu`)
  - `whisper_compute_type` (`auto|...`)
  - `transcription_language` (optional, например `ru`)
- В analysis/per-deal артефактах дополнительно:
  - `transcript_source`
  - `transcript_error`
- Если локальный audio path отсутствует или backend недоступен, pipeline не падает: возвращается controlled status, а анализ продолжает работать в прежнем режиме.

Практическая установка и проверка:
- Установка:
  - `python -m pip install faster-whisper`
- Проверка импорта:
  - `python -c "import faster_whisper; print('ok', faster_whisper.__version__)"`
- Если в конфиге выбран `transcription_backend=faster_whisper`, но пакет не установлен, в логах будет warning:
  - `faster-whisper is not installed or unavailable; transcription backend will run with controlled fallback`
  - run при этом не падает.

### Automatic call recording download (audio cache)
- Для call evidence добавлен авто-resolve аудио:
  - если `audio_path` уже есть и файл существует -> используется как есть;
  - иначе при наличии `recording_url` делается авто-скачивание в `workspace/deal_analyzer/audio_cache`;
  - имя файла детерминированное: `deal_<id>__call_<id>__<url_hash>.<ext>`.
- В call evidence сохраняются:
  - `audio_path`
  - `audio_source_url`
  - `audio_download_status` (`local_exists|cached|downloaded|missing_url|failed|resolved_file_url`)
  - `audio_download_error` (если download не удался)
- Повторные прогоны используют cached audio и не скачивают повторно.
- Ошибка download не валит run: транскрибация по такому звонку безопасно пропускается через текущий fallback.

## Боевая запись в РОКС 2026 (safe write)
- `analyze-period` может писать в боевой лист `Дневной контроль`.
- `analyze-weekly` может писать в боевой лист `Недельный свод менеджеров`.
- Для `Дневной контроль` по умолчанию включен `append`-режим:
  - запись идет ниже уже заполненных строк, начиная с первой свободной строки от `start_cell` (`A2` по умолчанию);
  - строка заголовков не перезаписывается;
  - существующие записи сверху не затираются.
- Overwrite-режим доступен только явным флагом `deal_analyzer_overwrite_mode=true`.
- Технические поля (`deal_id`, `artifact_path`, технические коды причин) в боевые колонки не выводятся.
- В `Ссылки на сделки` пишутся полные URL amoCRM (`.../leads/detail/<id>`), по одной ссылке в строке (через перенос строки в ячейке).
- Для daily-контроля строки собираются как ретро-пакеты по контрольным дням:
  - базово понедельник-пятница;
  - суббота добавляется только при фактической активности;
  - воскресенье не используется.
- Порядок строк в daily payload: сначала день, внутри дня менеджер.
- В daily packaging действует manager allowlist (по умолчанию: `Илья`, `Рустам`).
- В боевой daily-лист не попадают другие менеджеры, если они не добавлены в `daily_manager_allowlist` в analyzer config.
- Для dropdown-полей daily-листа используются стабильные значения:
  - `День`: `Понедельник..Суббота` (воскресенье не пишется);
  - `Менеджер`: нормализованные значения (`Илья`, `Рустам`, если распознаны);
  - `Роль менеджера`: `телемаркетолог` / `менеджер по продажам`.
- Analyze-period сначала пробует live-refresh пула сделок через amoCRM API за выбранный период (API-first), при неудаче безопасно откатывается на входной period JSON и явно пишет fallback в summary.
- Live-refresh можно отключить флагом `period_live_refresh_enabled=false` (для полностью офлайн прогона).
- Daily policy: call-first. Если есть звонок/транскрипт, он приоритетен для ключевого вывода, сильных сторон и зон роста. Если звонка нет — выводы сдержаннее и опираются на CRM.
- В debug payload для каждой daily-строки добавляется `selection_reason` (`has_call_priority` / `rich_context_priority` / `fallback_fill`), в таблицу это поле не пишется.
- User-facing формулировки в `Дневной контроль` проходят human rewrite-слой: без англо-терминов и канцелярита, в рабочем управленческом тоне.

### Mapping: Дневной контроль
Колонки заполняются строго в формате рабочего шаблона:
`A..U`:
- Неделя с
- Неделя по
- Дата контроля
- День
- Менеджер
- Роль менеджера
- Проанализировано сделок
- Ссылки на сделки
- Продукт / фокус
- База микс
- Ключевой вывод
- Сильные стороны
- Зоны роста
- Почему это важно
- Что закрепить
- Что исправить
- Что донес сотруднику
- Ожидаемый эффект - количество
- Ожидаемый эффект - качество
- Оценка 0-100
- Критичность

### Mapping: Недельный свод менеджеров
- Запись строго в утвержденные колонки `A:U`:
  - Неделя с, Неделя по, Менеджер, Роль менеджера, Проанализировано сделок,
  - Продукт / фокус недели, База микс недели, Итог недели, Что улучшилось, Что не улучшилось,
  - Повторяющиеся ошибки, Обучение сотруднику, Ссылка на обучение,
  - Задачи после обучения, Ссылка на задачи после обучения,
  - Мои действия на следующую неделю, Ожидаемый эффект - количество, Ожидаемый эффект - качество,
  - Формулировка для руководителя, Сообщение сотруднику, Средняя оценка 0-100.
- Технические поля в боевой лист не выводятся.


## Transcript Usability Layer (Daily Control Input)

Daily-control теперь разделяет:
- `transcription technically succeeded`
- `transcript is usable for analysis`

Для каждой сделки в debug-поле фиксируются:
- `transcript_text_len`
- `transcript_nonempty_ratio`
- `transcript_noise_score`
- `transcript_repeat_score`
- `transcript_signal_score`
- `transcript_usability_score_final`
- `transcript_usability_label` (`usable|weak|noisy|empty`)

В `summary.json -> transcript_runtime_diagnostics` добавлены:
- `transcriptions_usable`
- `transcriptions_weak`
- `transcriptions_noisy`
- `transcriptions_empty`
- `deals_with_usable_transcript`

## Daily Candidate Pool -> Ranking -> Top Selection

Daily-пакет формируется через:
1) candidate-pool по дню и менеджеру
2) rules prefilter (`transcript_usability_score`, `evidence_richness_score`, `funnel_relevance_score`, `management_value_score`)
3) optional LLM rerank только для ограниченного shortlist (обычно 8-12 кандидатов)
4) top selection без честного заполнения мусором

Если transcript слабый/шумный и CRM-контекст тонкий, сделка может быть помечена `skip_for_daily_reason` и не попадать в daily-строку.

Дополнительные debug-поля daily-строки:
- `daily_package_quality_label` (`strong|acceptable|thin|weak`)
- `daily_package_has_forced_fallback`
- `negotiation_signal_presence_score`
- `crm_only_bias_flag`
- `text_generation_source_per_column`
- `style_layer_applied`
- `transcript_quality_retry_used`
- `transcript_quality_retry_improved`

Для shortlist кандидатов фиксируются:
- `llm_daily_rank`
- `llm_daily_rank_reason`
- `llm_call_analysis_viability`
- `llm_call_analysis_viability_reason`

## Effect Forecast (Units + Cascade)

`Ожидаемый эффект - количество` строится каскадом в штуках:
- фиксируем проблемный этап,
- считаем абсолютный эффект на нем,
- протягиваем вниз по воронке.

Debug-поля в daily payload:
- `effect_forecast_source` (`roks|fallback`)
- `effect_problem_stage`
- `effect_downstream_stages`
- `stage_priority_weight_source`
- `stage_priority_weight_value`

Если метрики РОКС недоступны, включается консервативный fallback и это явно логируется.

## Optional Context Boost (not scoring truth)

В config доступны:
- `product_reference_urls` (`info|link|both`)
- `sales_module_references`

Эти поля передаются в LLM prompt как контекст/библиотека модулей, но не используются как жесткая scoring-истина.

## Style/Reference Sources (Daily Text)

При генерации daily-текста runtime пытается подхватить:
- `docs/мой паттерн общения.txt`
- `docs/style_sources/telegram_ilya/**/*.{txt,md,html}`
- `docs/sales_context/scripts/link_base.md`
- `docs/sales_context/scripts/info_plm_base.md`
- `docs/sales_context/scripts/info_plm_light_industry.md`
- все файлы из `sales_module_references`, если указан путь к папке (рекурсивно).

В лог пишется, какие style/reference sources реально загрузились.

## Daily LLM Failover + Write Guard

- Для `analyze-period` и daily-текста используется единый runtime-resolver:
  - main: `ollama_base_url` + `ollama_model`
  - fallback: `ollama_fallback_*`
- Выбор runtime фиксируется в `summary.json`:
  - `analysis_llm_runtime`
  - `daily_llm_runtime`
- Если main недоступна, но fallback жива: daily-генерация идет через fallback.
- Если обе недоступны:
  - daily J-Q не считаются готовыми,
  - запись в боевой лист принудительно уходит в dry-run,
  - статус writer: `write_forced_dry_run_no_live_llm`.

## Daily Multi-Step Pipeline (Debuggable)

Для daily generation включен последовательный конвейер:
1) candidate selection  
2) primary free-form analysis  
3) effect/motivation layer  
4) block split  
5) style rewrite  
6) final assembler (`source_of_truth=styled_blocks`, `assembler_only=true`)  
7) writer-ready handoff

Принцип:
- каждый шаг пишет отдельный debug artifact;
- если ломается любой шаг, строка не попадает в боевой payload;
- run продолжает работу в безопасном режиме (dry-run), с явным `failed_step` и `error`.

Где смотреть:
- `period_runs/<timestamp>/daily_step_artifacts/...` — артефакты по шагам;
- `daily_control_sheet_payload.json -> daily_multistep_pipeline`;
- `summary.json -> daily_multistep_pipeline`.

Этот слой не использует rules-текст как содержательный fallback для J-Q в LLM-ветке.  
Rules остаются только factual/debug основой (отбор/сигналы/guardrails).

## Native Writer: "Разбор звонков"

Active write-path для `analyze-period` теперь идет через нативный payload `call_review_sheet_payload.json`:
- одна строка = один case (`deal_id`) из shortlist;
- источник строк: `analysis_shortlist.json` + per-deal `period_deal_records`;
- legacy daily rows не используются как source of truth для схемы `Разбор звонков`.

Схема записи:
- writer читает реальные headers листа `Разбор звонков` перед записью;
- заполняет значения через header-aware mapper (включая дублирующиеся колонки вроде `Здоровается` и `Комментарий по этапу`);
- внутренние enum-коды (`warm_case`, `redial_discipline` и т.д.) не пишутся в user-facing ячейки.

Guardrails:
- `skip_no_meaningful_case` не пишется в боевой лист;
- при `daily_llm_runtime.selected=none` запись принудительно уходит в dry-run (`write_forced_dry_run_no_live_llm`);
- negotiation и discipline кейсы разведены и маркируются отдельно (`conversation_pool` vs `discipline_pool`).
- `--limit` режет только meaningful shortlist (`analysis_shortlist`), а не сырой period input;
- forced fallback в сырой CRM-пул отключен: если meaningful shortlist пуст, строк в боевой лист не будет.

## Call Review V2 Runtime Notes

- Active write-path для `analyze-period` изолирован на `call_review_writer`.
- `daily_control_writer` и `meeting_queue_writer` в этом режиме помечаются как `inactive_for_analyze_period`.
- Пишем только в лист `Разбор звонков`; если имя вкладки в конфиге повреждено, writer пробует fallback на штатное имя вкладки.
- Перед записью writer читает реальные headers и data validation текущей строки шаблона и нормализует значения к допустимым dropdown-опциям.

Business windows (MSK, cut-off 15:00):
- звонки после `15:00` относятся к следующему рабочему дню bucket;
- bucket текущего дня до `15:00` считается открытым и в боевую запись не идет;
- `Дата анализа` в строке берется из `business_window_date`, `Дата кейса` - из anchor-call даты.

LLM gating:
- user-facing контент строки пишется только при `call_review_llm_ready=true`;
- если live runtime недоступен (`selected=none`) - принудительный dry-run, без боевой записи;
- weak/noisy/empty conversation cases отсеиваются до writer.

Step artifacts:
- для call-review многошагового LLM-пайплайна артефакты пишутся в:
  `period_runs/<run_id>/call_review_step_artifacts/`.
- это позволяет видеть, на каком шаге выпал кейс (`free_form`, `effect_layer`, `structured_json`, `style_rewrite`, `validation`).

## CRM Consistency Layer (Parallel, not dominant)

CRM-анализ вынесен в отдельный параллельный слой и не должен доминировать над разбором переговоров при наличии живого звонка.

Новые поля:
- `crm_consistency_summary`
- `crm_hygiene_flags`
- `crm_vs_call_mismatch`
- `crm_consistency_debug`

Логика:
- если есть живой разговор, mismatch CRM vs call фиксируется отдельным блоком;
- если живого разговора нет, CRM/discpline слой может становиться основным источником сигнала.

## Deterministic Base Mix (Code-first)

`База микс` собирается детерминированно и без LLM:
1) `deal tags`
2) `company tags` (с безопасным auto-propagation в merged deal view)
3) source/form/url/title hints
4) company meaning / OKVED / comments hints
5) fallback `солянка`

Запрещено использовать status/stage как источник `База микс`.

## Daily Case Modes (Type-Specific Contract)

Daily-кейс перед генерацией типизируется:
- `negotiation_lpr_analysis`
- `secretary_analysis`
- `redial_discipline_analysis`
- `supplier_inbound_analysis`
- `warm_inbound_analysis`
- `skip_no_meaningful_case`

Для каждого режима в prompt передаются:
- `allowed_axes`
- `banned_topics`
- mode reason/confidence

Это убирает CRM-first спам и блокирует нерелевантные темы:
- secretary/redial кейсы не тянут demo/brief/боль как основной разбор;
- LPR-кейсы приоритетно разбираются по этапам разговора.
