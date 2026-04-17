from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class OllamaClientError(RuntimeError):
    pass


def parse_json_response(content: str) -> dict[str, Any]:
    text = (content or "").strip()
    if not text:
        raise OllamaClientError("Ollama content is empty")

    direct = _try_parse_object(text)
    if direct is not None:
        return direct

    # Simple first/last brace extraction.
    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last != -1 and last > first:
        candidate = text[first : last + 1]
        parsed = _try_parse_object(candidate)
        if parsed is not None:
            return parsed

    # Nested-brace extraction with string-aware scan.
    candidate = _extract_first_balanced_json_object(text)
    if candidate:
        parsed = _try_parse_object(candidate)
        if parsed is not None:
            return parsed

    preview = text[:300].replace("\n", " ")
    raise OllamaClientError(f"Ollama content is not valid JSON object: {preview}")


@dataclass(frozen=True)
class OllamaClient:
    base_url: str
    model: str
    timeout_seconds: int = 60

    def chat_json(self, *, messages: list[dict[str, str]]) -> dict[str, Any]:
        endpoint = self._chat_endpoint()
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "format": "json",
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            preview = ""
            try:
                preview = exc.read().decode("utf-8", errors="replace")
            except Exception:
                preview = ""
            raise OllamaClientError(
                f"Ollama HTTP error: status={exc.code} endpoint={endpoint} body={preview[:300]}"
            ) from exc
        except URLError as exc:
            raise OllamaClientError(
                f"Ollama connection failed: endpoint={endpoint} reason={getattr(exc, 'reason', exc)}"
            ) from exc

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise OllamaClientError(f"Ollama returned invalid JSON envelope: {raw[:300]}") from exc

        content = data.get("message", {}).get("content") if isinstance(data, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise OllamaClientError("Ollama response has no message.content JSON payload")

        return parse_json_response(content)

    def _chat_endpoint(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/chat"


def _try_parse_object(text: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_first_balanced_json_object(text: str) -> str | None:
    in_string = False
    escaped = False
    depth = 0
    start_idx: int | None = None

    for idx, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == "{":
            if depth == 0:
                start_idx = idx
            depth += 1
            continue

        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start_idx is not None:
                return text[start_idx : idx + 1]

    return None
