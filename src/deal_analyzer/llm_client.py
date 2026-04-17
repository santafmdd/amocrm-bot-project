from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class OllamaClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParsedJsonResponse:
    payload: dict[str, Any]
    repair_applied: bool


@dataclass(frozen=True)
class OllamaPreflightResult:
    ok: bool
    error: str | None
    response_ms: int | None = None


def parse_json_response(content: str) -> ParsedJsonResponse:
    text = (content or "").strip()
    if not text:
        raise OllamaClientError("Ollama content is empty")

    direct = _try_parse_object(text)
    if direct is not None:
        return ParsedJsonResponse(payload=direct, repair_applied=False)

    cleaned = _strip_code_fences(text)
    if cleaned != text:
        parsed = _try_parse_object(cleaned)
        if parsed is not None:
            return ParsedJsonResponse(payload=parsed, repair_applied=True)

    extracted = _extract_first_last_object_candidate(cleaned)
    if extracted:
        parsed = _try_parse_object(extracted)
        if parsed is not None:
            return ParsedJsonResponse(payload=parsed, repair_applied=True)

    candidate = _extract_first_balanced_json_object(cleaned)
    if candidate:
        parsed = _try_parse_object(candidate)
        if parsed is not None:
            return ParsedJsonResponse(payload=parsed, repair_applied=True)

    preview = cleaned[:300].replace("\n", " ")
    raise OllamaClientError(f"Ollama content is not valid JSON object: {preview}")


@dataclass(frozen=True)
class OllamaClient:
    base_url: str
    model: str
    timeout_seconds: int = 60

    def preflight(self, *, probe_timeout_seconds: int = 5) -> OllamaPreflightResult:
        endpoint = self._chat_endpoint()
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": "{}"}],
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
        timeout = max(1, int(probe_timeout_seconds))
        try:
            with urlopen(req, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            preview = ""
            try:
                preview = exc.read().decode("utf-8", errors="replace")
            except Exception:
                preview = ""
            return OllamaPreflightResult(
                ok=False,
                error=f"HTTP {exc.code}: {preview[:200]}",
            )
        except (URLError, TimeoutError) as exc:
            return OllamaPreflightResult(ok=False, error=f"connection error: {getattr(exc, 'reason', exc)}")

        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            return OllamaPreflightResult(ok=False, error=f"invalid JSON envelope: {raw[:200]}")

        content = envelope.get("message", {}).get("content") if isinstance(envelope, dict) else None
        if not isinstance(content, str) or not content.strip():
            return OllamaPreflightResult(ok=False, error="missing message.content")

        try:
            parse_json_response(content)
        except OllamaClientError as exc:
            return OllamaPreflightResult(ok=False, error=str(exc))

        return OllamaPreflightResult(ok=True, error=None)

    def chat_json(self, *, messages: list[dict[str, str]]) -> ParsedJsonResponse:
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
        except (URLError, TimeoutError) as exc:
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


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return text
    lines = stripped.splitlines()
    if not lines:
        return text
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_first_last_object_candidate(text: str) -> str | None:
    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last != -1 and last > first:
        return text[first : last + 1]
    return None


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
