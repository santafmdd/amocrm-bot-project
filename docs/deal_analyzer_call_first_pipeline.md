# Deal Analyzer: Call-First Pre-Limit Pipeline

## Цель

Перед ограничением `--limit` сделать прозрачный проход по всем сделкам периода и понять, где есть реальная звонковая фактура.

Это нужно, чтобы не резать пул слишком рано и не уходить в CRM-only шум, когда в периоде есть нормальные звонки.

## Порядок в `analyze-period`

1. Разрешение периода.
2. Live refresh из amoCRM API (или fallback на входной `--input`).
3. **Call metadata pass по всем сделкам периода (до limit).**
4. Split на `conversation_pool` и `discipline_pool`.
5. Формирование `transcription_shortlist` (только из `conversation_pool`).
6. Применение `--limit` к shortlist conversation pool (а не к сырому списку сделок).
7. Тяжелый per-deal этап: snapshot/transcription/analysis/export/write.

## Что собирается на pre-limit call pass

По каждой сделке:
- `deal_id`
- `owner_name`
- `status_name`
- `pipeline_name`
- `calls_total`
- `outbound_calls`
- `inbound_calls`
- `max_duration_seconds`
- `total_duration_seconds`
- `recording_url_count`
- `audio_path_count`
- `short_calls_0_20_count`
- `medium_calls_21_60_count`
- `long_calls_61_plus_count`
- `no_answer_like_count`
- `autoanswer_like_count`
- `repeated_dead_redial_count`
- `same_time_redial_pattern_flag`
- `unique_phone_count`
- `numbers_not_fully_covered_flag`

## Новые артефакты периода

- `workspace/deal_analyzer/period_runs/<run_id>/call_pool_debug.json`
- `workspace/deal_analyzer/period_runs/<run_id>/call_pool_debug.md`
- `workspace/deal_analyzer/period_runs/<run_id>/conversation_pool.json`
- `workspace/deal_analyzer/period_runs/<run_id>/conversation_pool.md`
- `workspace/deal_analyzer/period_runs/<run_id>/discipline_pool.json`
- `workspace/deal_analyzer/period_runs/<run_id>/discipline_pool.md`
- `workspace/deal_analyzer/period_runs/<run_id>/transcription_shortlist.json`
- `workspace/deal_analyzer/period_runs/<run_id>/transcription_shortlist.md`

## Схема двух пулов (без смешивания)

Для каждой сделки на pre-limit слое теперь сохраняются:
- `pool_type`
- `pool_reason`
- `pool_priority_score`
- `call_case_type`

`call_case_type`:
- `lpr_conversation`
- `secretary_case`
- `supplier_inbound`
- `warm_inbound`
- `redial_discipline`
- `autoanswer_noise`
- `unknown`

`conversation_pool` приоритет:
1. осмысленный разговор (`lpr_conversation`),
2. `secretary_case`,
3. `supplier_inbound` / `warm_inbound`,
4. затем остальные разговорные кейсы с записью/длительностью.

`discipline_pool` приоритет:
1. `redial_discipline`,
2. `autoanswer_noise`,
3. short/no-answer/busy/voicemail/autoanswer паттерны.

## Новые pre-limit агрегаты в `summary.json`

- `deals_total_before_limit`
- `deals_with_any_calls`
- `deals_with_recordings`
- `deals_with_long_calls`
- `deals_with_only_short_calls`
- `deals_with_autoanswer_pattern`
- `deals_with_redial_pattern`
- `conversation_pool_total`
- `discipline_pool_total`
- `lpr_conversation_total`
- `secretary_case_total`
- `supplier_inbound_total`
- `warm_inbound_total`
- `redial_discipline_total`
- `autoanswer_noise_total`

## Важное ограничение этого шага

- На pre-limit pass нет heavy STT.
- На pre-limit pass нет принудительного скачивания аудио.
- Writer/daily-sheet логика не меняется: этот слой только для диагностики и прозрачного входного call-пула.

## Как работает STT после изменения

- Основной STT поток берет только сделки из `conversation_pool`.
- `discipline_pool` не идет в основной STT поток.
- По сделке в STT идет shortlist звонков:
  - 1 лучший основной разговор;
  - + до 1-2 follow-up звонков по тому же кейсу;
  - short/no-answer/autoanswer шум отсеивается.
- Если shortlist пустой — pipeline не падает (controlled fallback без fake “переговорного” слоя).
