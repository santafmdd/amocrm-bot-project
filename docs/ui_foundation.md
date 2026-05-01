# UI Foundation (MVP)

`src/deal_analyzer/ui_foundation.py` - минимальный FastAPI backend для запуска существующих CLI-процессов как jobs.

## Safety model
- `real_write=false` по умолчанию.
- Для real-write обязательно:
  - `real_write=true`
  - корректный `confirmation_token` (`DEAL_ANALYZER_UI_CONFIRM_TOKEN`).
- UI блокирует structural operations policy.
- Job artifacts пишутся в `workspace/ui_jobs/<job_id>/`.

## Endpoints
- `GET /health`
- `GET /jobs`
- `GET /jobs/{id}`
- `POST /jobs/call-review`
- `POST /jobs/daily-control`
- `POST /jobs/week-plan`
- `POST /jobs/training-materials`
- `POST /jobs/weekly-manager-summary`
- `POST /jobs/week-summary`
- `POST /jobs/cache-cleanup-dry-run`

## Job model
- `id`
- `type`
- `status` (`queued|running|succeeded|failed|blocked`)
- `command`
- `started_at`, `finished_at`
- `progress`
- `run_dir`
- `rows_prepared`, `rows_written`, `rows_quarantined`
- `block_reason`
- `error`
- `artifact_paths`

## Start command
```powershell
python -m src.deal_analyzer.ui_foundation --host 127.0.0.1 --port 8010
```

## MVP roadmap
Текущий scope:
- запуск dry-run/real-write job через API;
- хранение статуса job и ссылок на artifacts;
- safety-confirmation для real-write.

Следующие шаги:
- более богатый web UI (формы + history + retry);
- визуализация progress/heartbeat;
- интеграция с employee_dashboard views;
- preflight templates для nightly orchestration.
