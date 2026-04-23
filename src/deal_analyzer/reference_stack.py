from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.config import load_config

from .config import DealAnalyzerConfig


def build_daily_reference_stack(
    *,
    cfg: DealAnalyzerConfig | None,
    factual_payload: dict[str, Any],
    logger: Any | None = None,
) -> dict[str, Any]:
    app = load_config()
    query = _build_reference_query(factual_payload=factual_payload, cfg=cfg)

    internal_sources = _collect_internal_reference_paths(cfg=cfg, project_root=app.project_root)
    internal_snippets = _collect_local_snippets(paths=internal_sources, query=query, top_k=8, logger=logger)
    role_snippets = _collect_role_context_snippets(factual_payload=factual_payload)
    product_snippets = _collect_product_reference_snippets(cfg=cfg)
    external = _external_retrieval(cfg=cfg, query=query, logger=logger)

    combined: list[dict[str, Any]] = []
    combined.extend(internal_snippets)
    combined.extend(role_snippets)
    combined.extend(product_snippets)
    if isinstance(external.get("snippets"), list):
        combined.extend([x for x in external["snippets"] if isinstance(x, dict)])

    prompt_snippets = combined[:12]
    source_order = ["internal_references", "role_context", "product_reference_urls", "external_retrieval_optional"]

    configured_product_urls_count = 0
    if cfg is not None and isinstance(getattr(cfg, "product_reference_urls", None), dict):
        configured_product_urls_count = sum(
            1 for x in getattr(cfg, "product_reference_urls", {}).values() if str(x or "").strip()
        )

    required_layers = {
        "internal_references": {
            "required": True,
            "sources_total": len(internal_sources),
            "snippets_used": len(internal_snippets),
            "ok": bool(internal_snippets),
        },
        "role_context": {
            "required": True,
            "snippets_used": len(role_snippets),
            "ok": bool(role_snippets),
        },
        "product_reference_urls": {
            "required": True,
            "urls_configured": int(configured_product_urls_count),
            "snippets_used": len(product_snippets),
            "ok": bool(product_snippets),
        },
    }

    prompt_layers = [str(x.get("layer") or "") for x in prompt_snippets if isinstance(x, dict)]
    layer_presence = {
        "internal_in_prompt": any(x == "internal" for x in prompt_layers),
        "role_context_in_prompt": any(x == "role_context" for x in prompt_layers),
        "product_reference_in_prompt": any(x == "product_url" for x in prompt_layers),
    }

    if logger is not None:
        logger.info(
            "daily reference stack built: snippets=%s internal=%s role=%s product=%s external_used=%s",
            len(prompt_snippets),
            required_layers["internal_references"]["ok"],
            required_layers["role_context"]["ok"],
            required_layers["product_reference_urls"]["ok"],
            bool(external.get("used")),
        )

    return {
        "query": query,
        "source_order": source_order,
        "required_layers": required_layers,
        "layer_presence": layer_presence,
        "internal_sources": [str(x) for x in internal_sources],
        "internal_sources_count": len(internal_sources),
        "internal_snippets": internal_snippets,
        "internal_snippets_count": len(internal_snippets),
        "role_snippets": role_snippets,
        "role_snippets_count": len(role_snippets),
        "product_snippets": product_snippets,
        "product_snippets_count": len(product_snippets),
        "external_retrieval": external,
        "prompt_snippets": prompt_snippets,
        "prompt_snippets_count": len(prompt_snippets),
    }


def build_reference_prompt_section(reference_stack: dict[str, Any]) -> str:
    if not isinstance(reference_stack, dict):
        return ""
    lines: list[str] = []
    order = reference_stack.get("source_order", [])
    if isinstance(order, list) and order:
        lines.append(f"??????? ??????????: {', '.join(str(x) for x in order)}")

    required = reference_stack.get("required_layers", {})
    if isinstance(required, dict) and required:
        for key in ("internal_references", "role_context", "product_reference_urls"):
            row = required.get(key, {})
            if not isinstance(row, dict):
                continue
            lines.append(
                f"required[{key}] ok={bool(row.get('ok'))} snippets={int(row.get('snippets_used', 0) or 0)}"
            )

    snippets = reference_stack.get("prompt_snippets", [])
    if isinstance(snippets, list):
        for idx, item in enumerate(snippets[:12], start=1):
            if not isinstance(item, dict):
                continue
            layer = str(item.get("layer") or "ref")
            src = str(item.get("source") or "")
            text = str(item.get("snippet") or "").strip()
            if not text:
                continue
            lines.append(f"{idx}. [{layer}] {src}: {text}")

    ext = reference_stack.get("external_retrieval", {})
    if isinstance(ext, dict):
        lines.append(
            "External retrieval: "
            f"enabled={bool(ext.get('enabled'))}, used={bool(ext.get('used'))}, reason={str(ext.get('reason') or '')}"
        )
    return "\n".join(lines).strip()


def _build_reference_query(*, factual_payload: dict[str, Any], cfg: DealAnalyzerConfig | None) -> str:
    parts: list[str] = []
    if cfg is not None and str(getattr(cfg, "external_retrieval_query_prefix", "")).strip():
        parts.append(str(getattr(cfg, "external_retrieval_query_prefix", "")).strip())
    for key in ("manager_name", "role", "product_focus", "base_mix", "selection_reason"):
        val = str(factual_payload.get(key) or "").strip()
        if val:
            parts.append(val)
    deals = factual_payload.get("deals", [])
    if isinstance(deals, list):
        for item in deals[:4]:
            if not isinstance(item, dict):
                continue
            parts.extend(
                x
                for x in (
                    str(item.get("call_summary") or "").strip(),
                    str(item.get("transcript_excerpt") or "").strip(),
                    str(item.get("status") or "").strip(),
                    str(item.get("pipeline") or "").strip(),
                )
                if x
            )
    text = " ".join(parts)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1200]


def _collect_internal_reference_paths(*, cfg: DealAnalyzerConfig | None, project_root: Path) -> list[Path]:
    paths: list[Path] = []
    defaults = [
        project_root / "docs" / "sales_context" / "scripts" / "link_base.md",
        project_root / "docs" / "sales_context" / "scripts" / "info_plm_base.md",
        project_root / "docs" / "sales_context" / "scripts" / "info_plm_light_industry.md",
    ]
    paths.extend(defaults)
    if cfg is not None:
        for raw in list(getattr(cfg, "sales_module_references", ()) or []):
            text = str(raw or "").strip()
            if not text:
                continue
            candidate = Path(text)
            if not candidate.is_absolute():
                candidate = (project_root / candidate).resolve()
            if candidate.is_dir():
                for suffix in ("*.md", "*.txt", "*.html"):
                    paths.extend(sorted(candidate.rglob(suffix)))
            else:
                paths.append(candidate)
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        p = str(path.resolve())
        if p in seen or not path.exists():
            continue
        seen.add(p)
        out.append(path)
    return out


def _collect_local_snippets(*, paths: list[Path], query: str, top_k: int, logger: Any | None) -> list[dict[str, Any]]:
    query_tokens = _query_tokens(query)
    scored: list[tuple[int, dict[str, Any]]] = []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            try:
                text = path.read_text(encoding="utf-8-sig")
            except Exception as exc:
                if logger is not None:
                    logger.warning("reference source unreadable: path=%s error=%s", path, exc)
                continue
        compact = re.sub(r"\s+", " ", text).strip()
        if not compact:
            continue
        snippet = compact[:900]
        score = _token_overlap_score(snippet, query_tokens)
        scored.append(
            (
                score,
                {
                    "layer": "internal",
                    "source": str(path),
                    "snippet": snippet,
                    "score": score,
                },
            )
        )
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[: max(1, top_k)]]


def _collect_product_reference_snippets(*, cfg: DealAnalyzerConfig | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    refs = dict(getattr(cfg, "product_reference_urls", {}) or {}) if cfg is not None else {}
    for key in ("info", "link", "both"):
        url = str(refs.get(key) or "").strip()
        if not url:
            continue
        out.append(
            {
                "layer": "product_url",
                "source": url,
                "snippet": f"??????????? ???????? {key}: {url}",
                "score": 1,
            }
        )
    return out


def _collect_role_context_snippets(*, factual_payload: dict[str, Any]) -> list[dict[str, Any]]:
    manager = str(factual_payload.get("manager_name") or "").strip()
    role = str(factual_payload.get("role") or "").strip()
    case_policy = factual_payload.get("case_policy", {}) if isinstance(factual_payload.get("case_policy"), dict) else {}
    mode = str(case_policy.get("daily_analysis_mode") or "").strip()
    allowed_axes = (
        [str(x).strip() for x in case_policy.get("allowed_axes", []) if str(x).strip()]
        if isinstance(case_policy.get("allowed_axes"), list)
        else []
    )
    banned_topics = (
        [str(x).strip() for x in case_policy.get("banned_topics", []) if str(x).strip()]
        if isinstance(case_policy.get("banned_topics"), list)
        else []
    )
    role_allowed_topics = (
        [str(x).strip() for x in factual_payload.get("role_allowed_topics", []) if str(x).strip()]
        if isinstance(factual_payload.get("role_allowed_topics"), list)
        else []
    )
    role_forbidden_topics = (
        [str(x).strip() for x in factual_payload.get("role_forbidden_topics", []) if str(x).strip()]
        if isinstance(factual_payload.get("role_forbidden_topics"), list)
        else []
    )

    if not manager and not role and not mode and not allowed_axes and not role_allowed_topics and not banned_topics:
        return []

    snippet = (
        f"role context: manager={manager or '-'}; role={role or '-'}; mode={mode or '-'}; "
        f"allowed_axes={', '.join(allowed_axes[:8]) if allowed_axes else '-'}; "
        f"banned_topics={', '.join(banned_topics[:8]) if banned_topics else '-'}; "
        f"role_allowed={', '.join(role_allowed_topics[:8]) if role_allowed_topics else '-'}; "
        f"role_blocked={', '.join(role_forbidden_topics[:8]) if role_forbidden_topics else '-'}"
    )
    return [{"layer": "role_context", "source": "factual_payload.role_context", "snippet": snippet[:900], "score": 1}]


def _external_retrieval(*, cfg: DealAnalyzerConfig | None, query: str, logger: Any | None) -> dict[str, Any]:
    if cfg is None:
        return {"enabled": False, "used": False, "reason": "no_cfg", "snippets": [], "sources": []}
    if not bool(getattr(cfg, "external_retrieval_enabled", False)):
        return {"enabled": False, "used": False, "reason": "disabled_by_config", "snippets": [], "sources": []}
    adapter = str(getattr(cfg, "external_retrieval_adapter", "none") or "none").strip().lower()
    if adapter != "http_json":
        return {
            "enabled": True,
            "used": False,
            "adapter": adapter,
            "reason": "adapter_not_supported",
            "snippets": [],
            "sources": [],
        }
    endpoint = str(getattr(cfg, "external_retrieval_endpoint", "") or "").strip()
    if not endpoint:
        return {
            "enabled": True,
            "used": False,
            "adapter": adapter,
            "reason": "missing_endpoint",
            "snippets": [],
            "sources": [],
        }
    top_k = int(getattr(cfg, "external_retrieval_top_k", 3) or 3)
    timeout_s = int(getattr(cfg, "external_retrieval_timeout_seconds", 10) or 10)
    api_key = str(getattr(cfg, "external_retrieval_api_key", "") or "").strip()
    req_payload = {"query": query, "top_k": top_k}
    body = json.dumps(req_payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = Request(endpoint, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=max(1, timeout_s)) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        preview = ""
        try:
            preview = exc.read().decode("utf-8", errors="replace")
        except Exception:
            preview = ""
        return {
            "enabled": True,
            "used": False,
            "adapter": adapter,
            "endpoint": endpoint,
            "reason": f"http_{exc.code}",
            "error": preview[:300],
            "snippets": [],
            "sources": [],
        }
    except (URLError, TimeoutError) as exc:
        return {
            "enabled": True,
            "used": False,
            "adapter": adapter,
            "endpoint": endpoint,
            "reason": "connection_error",
            "error": str(getattr(exc, "reason", exc)),
            "snippets": [],
            "sources": [],
        }
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "enabled": True,
            "used": False,
            "adapter": adapter,
            "endpoint": endpoint,
            "reason": "invalid_json",
            "snippets": [],
            "sources": [],
        }

    items = parsed.get("items") if isinstance(parsed, dict) else parsed
    snippets: list[dict[str, Any]] = []
    sources: list[str] = []
    if isinstance(items, list):
        for item in items[: max(1, top_k)]:
            if isinstance(item, dict):
                text = str(item.get("snippet") or item.get("text") or "").strip()
                src = str(item.get("source") or item.get("url") or endpoint).strip()
            else:
                text = str(item or "").strip()
                src = endpoint
            if not text:
                continue
            snippets.append({"layer": "external", "source": src, "snippet": text[:700], "score": 1})
            sources.append(src)
    used = len(snippets) > 0
    if logger is not None:
        logger.info(
            "external retrieval: enabled=true used=%s adapter=%s endpoint=%s snippets=%s",
            used,
            adapter,
            endpoint,
            len(snippets),
        )
    return {
        "enabled": True,
        "used": used,
        "adapter": adapter,
        "endpoint": endpoint,
        "reason": "ok" if used else "empty_result",
        "snippets": snippets,
        "sources": sources,
    }


def _query_tokens(query: str) -> set[str]:
    tokens = set(re.findall(r"[a-zA-Z?-??-?0-9]{4,}", str(query or "").lower()))
    return {t for t in tokens if t not in {"????????", "??????", "????????", "??????", "??????"}}


def _token_overlap_score(text: str, query_tokens: set[str]) -> int:
    if not query_tokens:
        return 0
    low = str(text or "").lower()
    return sum(1 for tok in query_tokens if tok in low)
