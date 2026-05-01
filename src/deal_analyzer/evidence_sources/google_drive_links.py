from __future__ import annotations

import re
from typing import Any

from .models import GoogleDriveLink


_URL_RE = re.compile(r"(https?://[^\s<>\"']+)", re.IGNORECASE)
_GOOGLE_DRIVE_HOSTS = ("drive.google.com", "docs.google.com")
_FILE_ID_PATTERNS = (
    re.compile(r"/file/d/([a-zA-Z0-9_-]{10,})", re.IGNORECASE),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]{10,})", re.IGNORECASE),
    re.compile(r"/open\?id=([a-zA-Z0-9_-]{10,})", re.IGNORECASE),
)


def extract_google_drive_links_from_text(*, text: str, source_field: str) -> list[GoogleDriveLink]:
    raw = str(text or "")
    if not raw.strip():
        return []
    out: list[GoogleDriveLink] = []
    for match in _URL_RE.finditer(raw):
        url = str(match.group(1) or "").strip().rstrip(".,);")
        if not _is_google_link(url):
            continue
        file_id = _extract_file_id(url)
        kind = classify_google_drive_link_kind(url=url)
        excerpt = _source_excerpt(raw=raw, start=match.start(), end=match.end())
        out.append(
            GoogleDriveLink(
                url=url,
                file_id=file_id,
                kind=kind,
                source_field=source_field,
                source_excerpt=excerpt,
            )
        )
    return _dedupe_links(out)


def classify_google_drive_link_kind(*, url: str) -> str:
    value = str(url or "").lower()
    if "/folders/" in value:
        return "folder"
    if any(token in value for token in (".mp4", ".mov", ".mkv", ".avi", "video")):
        return "video"
    if any(token in value for token in (".mp3", ".wav", ".m4a", ".ogg", "audio")):
        return "audio"
    if any(token in value for token in ("/document/", "/spreadsheets/", "/presentation/", ".pdf", ".doc", ".docx")):
        return "doc"
    if "/file/d/" in value or "open?id=" in value:
        return "unknown"
    return "unknown"


def collect_links_from_mapping(
    *,
    record: dict[str, Any],
    fields: list[str] | tuple[str, ...] | None = None,
) -> list[GoogleDriveLink]:
    out: list[GoogleDriveLink] = []
    if not isinstance(record, dict):
        return out
    selected_fields = [str(x).strip() for x in (fields or []) if str(x).strip()]
    if not selected_fields:
        selected_fields = list(record.keys())
    for field in selected_fields:
        value = record.get(field)
        if isinstance(value, list):
            joined = "\n".join(str(x or "") for x in value)
            out.extend(extract_google_drive_links_from_text(text=joined, source_field=field))
        elif isinstance(value, dict):
            joined = "\n".join(f"{k}: {v}" for k, v in value.items())
            out.extend(extract_google_drive_links_from_text(text=joined, source_field=field))
        else:
            out.extend(extract_google_drive_links_from_text(text=str(value or ""), source_field=field))
    return _dedupe_links(out)


def _source_excerpt(*, raw: str, start: int, end: int, radius: int = 140) -> str:
    left = max(0, int(start) - radius)
    right = min(len(raw), int(end) + radius)
    return " ".join(str(raw[left:right]).split()).strip()


def _extract_file_id(url: str) -> str:
    value = str(url or "")
    for pattern in _FILE_ID_PATTERNS:
        match = pattern.search(value)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _is_google_link(url: str) -> bool:
    value = str(url or "").lower()
    return any(host in value for host in _GOOGLE_DRIVE_HOSTS)


def _dedupe_links(items: list[GoogleDriveLink]) -> list[GoogleDriveLink]:
    seen: set[tuple[str, str]] = set()
    out: list[GoogleDriveLink] = []
    for item in items:
        key = (str(item.url).strip(), str(item.source_field).strip())
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out

