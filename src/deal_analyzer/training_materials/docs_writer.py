from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import AUTH_MODE_CACHE_ONLY, AUTH_MODE_INTERACTIVE_BOOTSTRAP, GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text
from .models import TrainingDraft

REQUIRED_SCOPES = {
    "spreadsheets": "https://www.googleapis.com/auth/spreadsheets",
    "drive_file": "https://www.googleapis.com/auth/drive.file",
    "documents": "https://www.googleapis.com/auth/documents",
}


def _read_token_scopes(token_file: Path) -> list[str]:
    if not token_file.exists():
        return []
    try:
        payload = json.loads(token_file.read_text(encoding="utf-8"))
    except Exception:
        return []
    scopes = payload.get("scopes") if isinstance(payload, dict) else []
    if isinstance(scopes, list):
        return [str(x).strip() for x in scopes if str(x).strip()]
    if isinstance(scopes, str) and scopes.strip():
        return [scopes.strip()]
    return []


def detect_google_api_capabilities(*, project_root: Path) -> dict[str, Any]:
    token_file = project_root / "token.json"
    credentials_file = project_root / "credentials.json"
    token_scopes = _read_token_scopes(token_file)

    has_sheets_scope = REQUIRED_SCOPES["spreadsheets"] in token_scopes
    has_drive_scope = REQUIRED_SCOPES["drive_file"] in token_scopes
    has_docs_scope = REQUIRED_SCOPES["documents"] in token_scopes

    libraries_available = True
    libs_error = ""
    try:
        import googleapiclient.discovery  # noqa: F401
        import google.oauth2.credentials  # noqa: F401
    except Exception as exc:  # pragma: no cover - depends on env
        libraries_available = False
        libs_error = str(exc)

    docs_api_available = bool(libraries_available and has_drive_scope and has_docs_scope and token_file.exists() and credentials_file.exists())
    missing_scopes = [scope for scope in REQUIRED_SCOPES.values() if scope not in token_scopes]

    scope_mismatch_detected = bool(token_file.exists() and missing_scopes)
    reauth_required = bool(scope_mismatch_detected)
    reauth_instruction = "Удалите token.json и пройдите OAuth заново" if reauth_required else ""

    return {
        "token_file": str(token_file),
        "credentials_file": str(credentials_file),
        "token_file_exists": token_file.exists(),
        "credentials_file_exists": credentials_file.exists(),
        "token_scopes": token_scopes,
        "libraries_available": libraries_available,
        "libraries_error": libs_error,
        "has_sheets_scope": has_sheets_scope,
        "has_drive_scope": has_drive_scope,
        "has_docs_scope": has_docs_scope,
        "docs_api_available": docs_api_available,
        "required_scopes": list(REQUIRED_SCOPES.values()),
        "missing_scopes": missing_scopes,
        "status": "docs_api_available" if docs_api_available else "docs_api_unavailable",
        "scope_mismatch_detected": scope_mismatch_detected,
        "reauth_required": reauth_required,
        "reauth_instruction": reauth_instruction,
    }


def training_materials_required_scopes() -> list[str]:
    return [
        REQUIRED_SCOPES["spreadsheets"],
        REQUIRED_SCOPES["drive_file"],
        REQUIRED_SCOPES["documents"],
    ]


def check_training_materials_oauth_scopes(*, project_root: Path, logger: Any = None) -> dict[str, Any]:
    status = detect_google_api_capabilities(project_root=project_root)
    try:
        probe_client = GoogleSheetsApiClient(
            project_root=project_root,
            logger=logger,
            scopes=training_materials_required_scopes(),
            auth_mode=AUTH_MODE_CACHE_ONLY,
        )
        probe_client.build_service()
        status["scope_request_probe_status"] = "ok"
        status["scope_request_probe_error"] = ""
    except Exception as exc:
        status["scope_request_probe_status"] = "failed"
        status["scope_request_probe_error"] = str(exc)
        if status.get("scope_mismatch_detected", False):
            status["reauth_required"] = True
            status["reauth_instruction"] = "Удалите token.json и пройдите OAuth заново"
    return status


def ensure_training_materials_oauth_scopes(
    *,
    project_root: Path,
    logger: Any = None,
    force_reauth: bool = False,
) -> dict[str, Any]:
    status = check_training_materials_oauth_scopes(project_root=project_root, logger=logger)
    if not bool(force_reauth):
        if bool(status.get("docs_api_available", False)):
            status["status"] = "ok"
        return status

    token_file = Path(str(status.get("token_file") or (project_root / "token.json")))
    backup_path = ""
    try:
        if token_file.exists():
            backup_name = f"{token_file.stem}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}{token_file.suffix}"
            backup_file = token_file.with_name(backup_name)
            token_file.replace(backup_file)
            backup_path = str(backup_file)
        client = GoogleSheetsApiClient(
            project_root=project_root,
            logger=logger,
            scopes=training_materials_required_scopes(),
            auth_mode=AUTH_MODE_INTERACTIVE_BOOTSTRAP,
        )
        client.build_service()
        refreshed = check_training_materials_oauth_scopes(project_root=project_root, logger=logger)
        refreshed["force_reauth_attempted"] = True
        refreshed["force_reauth_status"] = "ok"
        refreshed["force_reauth_error"] = ""
        refreshed["token_backup_file"] = backup_path
        if bool(refreshed.get("docs_api_available", False)):
            refreshed["status"] = "ok"
        return refreshed
    except Exception as exc:
        status["force_reauth_attempted"] = True
        status["force_reauth_status"] = "failed"
        status["force_reauth_error"] = str(exc)
        status["token_backup_file"] = backup_path
        if bool(status.get("docs_api_available", False)):
            status["status"] = "ok"
        return status


def build_training_markdown(*, draft: TrainingDraft) -> str:
    c = draft.candidate
    body = str(draft.training_material or "").strip()
    lines = [
        f"# {clean_text(draft.training_title)}",
        "",
        "## Для кого",
        f"- Сотрудник: {clean_text(c.recipient)}",
        f"- Неделя: {clean_text(c.plan_week_start)}..{clean_text(c.plan_week_end)}",
        f"- Дата обучения: {clean_text(c.plan_date)}",
        "",
        "## Источник проблемы",
        "- План недели",
        "- Дневной контроль",
        "- Разбор звонков",
        "",
        "## Контекст строки плана",
        f"- Тип активности: {clean_text(c.activity_type)}",
        f"- Что делаю: {clean_text(c.what_i_do)}",
        f"- Какую задачу даю: {clean_text(c.task_to_assign)}",
        f"- Что проверяю: {clean_text(c.what_to_check)}",
        f"- Тезис на дейлик: {clean_text(c.daily_meeting_thesis)}",
        "",
    ]
    if body:
        lines.append(body)
        lines.append("")
    lines.extend(
        [
            "## Ожидаемый эффект",
            f"- Количество: {clean_text(c.expected_quantity_effect)}",
            f"- Качество: {clean_text(c.expected_quality_effect)}",
        ]
    )
    return "\n".join(lines).strip()


def build_task_markdown(*, draft: TrainingDraft) -> str:
    c = draft.candidate
    body = str(draft.task_material or "").strip()
    lines = [
        f"# {clean_text(draft.task_title)}",
        "",
        "## Для кого",
        f"- Сотрудник: {clean_text(c.recipient)}",
        f"- Плановая дата: {clean_text(c.plan_date)}",
        f"- Срок: {clean_text(c.plan_week_end)}",
        "",
        "## Основание",
        f"- Что делаю: {clean_text(c.what_i_do)}",
        f"- Какую задачу даю: {clean_text(c.task_to_assign)}",
        f"- Что проверяю: {clean_text(c.what_to_check)}",
        "",
    ]
    if body:
        lines.append(body)
        lines.append("")
    lines.extend(
        [
            "## Критерии контроля",
            "- Проверяем факты внедрения в реальных звонках.",
            "- Проверяем фиксацию результата в CRM.",
            "- Проверяем наличие следующего шага и срока.",
        ]
    )
    return "\n".join(lines).strip()


def prepare_local_docs(*, drafts: list[TrainingDraft], run_dir: Path) -> list[dict[str, Any]]:
    docs_dir = run_dir / "generated_docs"
    tasks_dir = run_dir / "generated_tasks"
    docs_dir.mkdir(parents=True, exist_ok=True)
    tasks_dir.mkdir(parents=True, exist_ok=True)

    prepared: list[dict[str, Any]] = []
    for idx, draft in enumerate(drafts, start=1):
        file_stub = f"{draft.candidate.plan_week_start}_{draft.candidate.plan_date}_{idx:03d}_{draft.candidate.topic_hash or 'topic'}"
        doc_path = docs_dir / f"training_{file_stub}.md"
        task_path = tasks_dir / f"task_{file_stub}.md"
        doc_path.write_text(build_training_markdown(draft=draft), encoding="utf-8")
        task_path.write_text(build_task_markdown(draft=draft), encoding="utf-8")
        prepared.append(
            {
                "row_number": draft.candidate.row_number,
                "idempotency_key": draft.candidate.idempotency_key,
                "recipient": draft.candidate.recipient,
                "plan_date": draft.candidate.plan_date,
                "training_title": clean_text(draft.training_title),
                "task_title": clean_text(draft.task_title),
                "training_doc_local_path": str(doc_path),
                "task_doc_local_path": str(task_path),
                "training_link": "",
                "post_training_task_link": "",
                "docs_status": "local_docs_created",
            }
        )
    return prepared


def _created_docs_artifact_path(run_dir: Path) -> Path:
    return run_dir / "training_materials_created_docs.json"


def load_created_docs_artifact(run_dir: Path) -> dict[str, Any]:
    path = _created_docs_artifact_path(run_dir)
    if not path.exists():
        return {"docs_by_idempotency_key": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"docs_by_idempotency_key": {}}
    if not isinstance(payload, dict):
        return {"docs_by_idempotency_key": {}}
    docs = payload.get("docs_by_idempotency_key")
    if not isinstance(docs, dict):
        payload["docs_by_idempotency_key"] = {}
    return payload


def save_created_docs_artifact(run_dir: Path, payload: dict[str, Any]) -> None:
    path = _created_docs_artifact_path(run_dir)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_docs_and_drive_services(*, project_root: Path, logger: Any = None):
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except Exception as exc:  # pragma: no cover - depends on env
        raise RuntimeError("Google Docs/Drive libraries are unavailable") from exc

    token_file = project_root / "token.json"
    if not token_file.exists():
        raise RuntimeError(f"Token file not found: {token_file}")

    creds = Credentials.from_authorized_user_file(str(token_file), training_materials_required_scopes())
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_file.write_text(creds.to_json(), encoding="utf-8")
        if logger is not None:
            logger.info("training_materials docs auth: refresh token updated")
    if not creds or not creds.valid:
        raise RuntimeError("Google credentials invalid for Docs/Drive scopes")

    docs_service = build("docs", "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return docs_service, drive_service


def _read_google_project_id(project_root: Path) -> str:
    credentials_file = project_root / "credentials.json"
    if not credentials_file.exists():
        return ""
    try:
        payload = json.loads(credentials_file.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    for block_key in ("installed", "web"):
        block = payload.get(block_key)
        if isinstance(block, dict):
            candidate = clean_text(block.get("project_id", ""))
            if candidate:
                return candidate
    return clean_text(payload.get("project_id", ""))


def _short_error_message(value: str, *, max_chars: int = 280) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max(64, max_chars - 3)] + "..."


def classify_docs_api_error(*, error: Exception, project_root: Path) -> dict[str, str]:
    raw = str(error or "")
    low = raw.lower()
    message = _short_error_message(raw)
    project_id = _read_google_project_id(project_root) or "<project_id>"

    service_disabled = ("service_disabled" in low) or ("accessnotconfigured" in low) or ("api has not been used in project" in low)
    docs_disabled = ("docs.googleapis.com" in low) or (("docs api" in low) and ("403" in low))
    drive_disabled = ("drive.googleapis.com" in low) or (("drive api" in low) and ("403" in low))

    if service_disabled and docs_disabled:
        return {
            "docs_api_status": "service_disabled",
            "drive_api_status": "unknown",
            "docs_api_error_type": "google_docs_api_disabled",
            "action_required": f"Enable Google Docs API in Google Cloud project {project_id}",
            "error_message_short": message or "Google Docs API disabled",
        }
    if service_disabled and drive_disabled:
        return {
            "docs_api_status": "unknown",
            "drive_api_status": "service_disabled",
            "docs_api_error_type": "google_drive_api_disabled",
            "action_required": f"Enable Google Drive API in Google Cloud project {project_id}",
            "error_message_short": message or "Google Drive API disabled",
        }
    return {
        "docs_api_status": "error",
        "drive_api_status": "error",
        "docs_api_error_type": "google_api_error",
        "action_required": "",
        "error_message_short": message or "Google API error",
    }


def _create_google_doc_with_text(*, docs_service: Any, title: str, text: str) -> tuple[str, str]:
    created = docs_service.documents().create(body={"title": title}).execute()
    document_id = str(created.get("documentId") or "").strip()
    if not document_id:
        raise RuntimeError("Google Docs create returned empty documentId")
    content = str(text or "").strip()
    if content:
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={"requests": [{"insertText": {"location": {"index": 1}, "text": content}}]},
        ).execute()
    url = f"https://docs.google.com/document/d/{document_id}/edit"
    return document_id, url


def _move_doc_to_folder(*, drive_service: Any, file_id: str, folder_id: str) -> None:
    target_folder = clean_text(folder_id)
    if not target_folder:
        return
    meta = drive_service.files().get(fileId=file_id, fields="parents").execute()
    parents = meta.get("parents", []) if isinstance(meta, dict) else []
    remove_parents = ",".join([str(item).strip() for item in parents if str(item).strip()])
    kwargs: dict[str, Any] = {"fileId": file_id, "addParents": target_folder, "fields": "id,parents"}
    if remove_parents:
        kwargs["removeParents"] = remove_parents
    drive_service.files().update(**kwargs).execute()


def materialize_docs_for_write(
    *,
    cfg: Any,
    run_dir: Path,
    payload_rows: list[dict[str, Any]],
    write_enabled: bool,
    overwrite_links: bool,
    logger: Any = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = [dict(item) for item in payload_rows if isinstance(item, dict)]
    created_artifact = load_created_docs_artifact(run_dir)
    docs_by_key = created_artifact.get("docs_by_idempotency_key", {})
    if not isinstance(docs_by_key, dict):
        docs_by_key = {}
        created_artifact["docs_by_idempotency_key"] = docs_by_key

    stats = {
        "docs_creation_mode": "write" if write_enabled else "dry_run",
        "rows_docs_created": 0,
        "rows_task_docs_created": 0,
        "rows_links_ready_to_write": 0,
        "docs_creation_errors_count": 0,
        "docs_creation_error_examples": [],
        "rows_docs_reused_from_artifact": 0,
        "docs_api_status": "not_checked" if write_enabled else "dry_run",
        "drive_api_status": "not_checked" if write_enabled else "dry_run",
        "docs_api_error_type": "",
        "action_required": "",
        "docs_api_error_message": "",
    }

    if not write_enabled:
        for row in rows:
            existing_training = clean_text(row.get("existing_training_link", ""))
            existing_task = clean_text(row.get("existing_post_training_task_link", ""))
            key = clean_text(row.get("idempotency_key", ""))
            artifact_entry = docs_by_key.get(key, {}) if key else {}
            if not isinstance(artifact_entry, dict):
                artifact_entry = {}
            if not clean_text(row.get("training_link", "")):
                if overwrite_links or not existing_training:
                    row["training_link"] = clean_text(artifact_entry.get("training_link", ""))
            if not clean_text(row.get("post_training_task_link", "")):
                if overwrite_links or not existing_task:
                    row["post_training_task_link"] = clean_text(artifact_entry.get("post_training_task_link", ""))
            if clean_text(row.get("training_link", "")) or clean_text(row.get("post_training_task_link", "")):
                stats["rows_links_ready_to_write"] += 1
        return rows, stats

    need_remote_creation = False
    for probe_row in rows:
        existing_training = clean_text(probe_row.get("existing_training_link", ""))
        existing_task = clean_text(probe_row.get("existing_post_training_task_link", ""))
        key = clean_text(probe_row.get("idempotency_key", ""))
        artifact_entry = docs_by_key.get(key, {}) if key else {}
        if not isinstance(artifact_entry, dict):
            artifact_entry = {}

        should_prepare_training = bool(overwrite_links or (not existing_training))
        should_prepare_task = bool(overwrite_links or (not existing_task))
        current_training = clean_text(probe_row.get("training_link", ""))
        current_task = clean_text(probe_row.get("post_training_task_link", ""))
        cached_training = clean_text(artifact_entry.get("training_link", ""))
        cached_task = clean_text(artifact_entry.get("post_training_task_link", ""))

        if should_prepare_training and (not current_training) and (not cached_training):
            need_remote_creation = True
            break
        if should_prepare_task and (not current_task) and (not cached_task):
            need_remote_creation = True
            break

    docs_service = None
    drive_service = None
    folder_id = clean_text(getattr(cfg, "training_materials_drive_folder_id", ""))
    if not need_remote_creation and write_enabled:
        stats["docs_api_status"] = "not_required"
        stats["drive_api_status"] = "not_required"
    if need_remote_creation:
        app_root = Path(cfg.config_path).resolve().parents[1]
        try:
            docs_service, drive_service = _build_docs_and_drive_services(project_root=app_root, logger=logger)
            stats["docs_api_status"] = "ok"
            stats["drive_api_status"] = "ok"
        except Exception as exc:
            classified = classify_docs_api_error(error=exc, project_root=app_root)
            stats["docs_api_status"] = classified.get("docs_api_status", "error")
            stats["drive_api_status"] = classified.get("drive_api_status", "error")
            stats["docs_api_error_type"] = classified.get("docs_api_error_type", "google_api_error")
            stats["action_required"] = classified.get("action_required", "")
            stats["docs_api_error_message"] = classified.get("error_message_short", "")
            stats["docs_creation_errors_count"] += len(rows)
            if len(stats["docs_creation_error_examples"]) < 10:
                stats["docs_creation_error_examples"].append(
                    {
                        "idempotency_key": "",
                        "row_number": 0,
                        "recipient": "",
                        "error": stats["docs_api_error_message"] or "Google API error",
                    }
                )
            return rows, stats

    for row in rows:
        key = clean_text(row.get("idempotency_key", ""))
        existing_training = clean_text(row.get("existing_training_link", ""))
        existing_task = clean_text(row.get("existing_post_training_task_link", ""))
        artifact_entry = docs_by_key.get(key, {}) if key else {}
        if not isinstance(artifact_entry, dict):
            artifact_entry = {}

        should_prepare_training = bool(overwrite_links or (not existing_training))
        should_prepare_task = bool(overwrite_links or (not existing_task))

        training_link = clean_text(row.get("training_link", "")) if should_prepare_training else ""
        task_link = clean_text(row.get("post_training_task_link", "")) if should_prepare_task else ""

        if should_prepare_training and (not training_link):
            cached = clean_text(artifact_entry.get("training_link", ""))
            if cached:
                row["training_link"] = cached
                training_link = cached
                stats["rows_docs_reused_from_artifact"] += 1
        if should_prepare_task and (not task_link):
            cached = clean_text(artifact_entry.get("post_training_task_link", ""))
            if cached:
                row["post_training_task_link"] = cached
                task_link = cached
                stats["rows_docs_reused_from_artifact"] += 1

        try:
            if should_prepare_training and not training_link:
                if docs_service is None or drive_service is None:
                    raise RuntimeError("Google Docs services are not initialized")
                title = clean_text(row.get("training_title", "")) or f"Обучение {clean_text(row.get('recipient', ''))}".strip()
                body = str(row.get("training_material", "") or "").strip()
                doc_id, url = _create_google_doc_with_text(docs_service=docs_service, title=title, text=body)
                _move_doc_to_folder(drive_service=drive_service, file_id=doc_id, folder_id=folder_id)
                row["training_link"] = url
                training_link = url
                stats["rows_docs_created"] += 1
                artifact_entry["training_doc_id"] = doc_id
                artifact_entry["training_link"] = url

            if should_prepare_task and not task_link:
                if docs_service is None or drive_service is None:
                    raise RuntimeError("Google Docs services are not initialized")
                title = clean_text(row.get("task_title", "")) or f"Задание после обучения {clean_text(row.get('recipient', ''))}".strip()
                body = str(row.get("task_material", "") or "").strip()
                doc_id, url = _create_google_doc_with_text(docs_service=docs_service, title=title, text=body)
                _move_doc_to_folder(drive_service=drive_service, file_id=doc_id, folder_id=folder_id)
                row["post_training_task_link"] = url
                task_link = url
                stats["rows_task_docs_created"] += 1
                artifact_entry["task_doc_id"] = doc_id
                artifact_entry["post_training_task_link"] = url
        except Exception as exc:
            app_root = Path(cfg.config_path).resolve().parents[1]
            classified = classify_docs_api_error(error=exc, project_root=app_root)
            stats["docs_api_status"] = classified.get("docs_api_status", stats.get("docs_api_status", "error"))
            stats["drive_api_status"] = classified.get("drive_api_status", stats.get("drive_api_status", "error"))
            stats["docs_api_error_type"] = classified.get("docs_api_error_type", stats.get("docs_api_error_type", "google_api_error"))
            if not stats.get("action_required"):
                stats["action_required"] = classified.get("action_required", "")
            short_error = classified.get("error_message_short", _short_error_message(str(exc)))
            if short_error and not stats.get("docs_api_error_message"):
                stats["docs_api_error_message"] = short_error
            stats["docs_creation_errors_count"] += 1
            if len(stats["docs_creation_error_examples"]) < 10:
                stats["docs_creation_error_examples"].append(
                    {
                        "idempotency_key": key,
                        "row_number": row.get("row_number", 0),
                        "recipient": row.get("recipient", ""),
                        "error": short_error or _short_error_message(str(exc)),
                    }
                )

        if key:
            artifact_entry.update(
                {
                    "idempotency_key": key,
                    "week_start": clean_text(row.get("plan_week_start", "")),
                    "week_end": clean_text(row.get("plan_week_end", "")),
                    "plan_date": clean_text(row.get("plan_date", "")),
                    "manager": clean_text(row.get("recipient", "")),
                    "activity_type": clean_text(row.get("activity_type", "")),
                    "topic_hash": clean_text(row.get("topic_hash", "")),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            docs_by_key[key] = artifact_entry

        if (should_prepare_training and clean_text(row.get("training_link", ""))) or (
            should_prepare_task and clean_text(row.get("post_training_task_link", ""))
        ):
            stats["rows_links_ready_to_write"] += 1

    created_artifact["docs_by_idempotency_key"] = docs_by_key
    created_artifact["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_created_docs_artifact(run_dir, created_artifact)
    return rows, stats
