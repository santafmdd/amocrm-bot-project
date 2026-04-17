from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class TranscriptCache:
    def __init__(self, *, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def make_key(self, call: dict[str, Any]) -> str:
        return build_transcript_cache_key(call)

    def get(self, key: str) -> dict[str, Any] | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def set(self, key: str, payload: dict[str, Any]) -> Path:
        path = self.cache_dir / f"{key}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path


def build_transcript_cache_key(call: dict[str, Any]) -> str:
    parts = [
        str(call.get("call_id") or ""),
        str(call.get("deal_id") or ""),
        str(call.get("recording_url") or "").strip().lower(),
        str(call.get("recording_ref") or "").strip().lower(),
        str(call.get("duration_seconds") or "0"),
        str(call.get("timestamp") or ""),
    ]
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
