from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import load_deal_analyzer_config
from .llm_runtime import resolve_ollama_runtime


def _chat_text(*, base_url: str, model: str, timeout_seconds: int, messages: list[dict[str, str]]) -> str:
    endpoint = f"{base_url.rstrip('/')}/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=max(1, int(timeout_seconds))) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        preview = ""
        try:
            preview = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"Ollama HTTP {exc.code}: {preview[:300]}") from exc
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"Ollama connection failed: {getattr(exc, 'reason', exc)}") from exc

    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Ollama invalid JSON envelope: {raw[:300]}") from exc
    content = envelope.get("message", {}).get("content") if isinstance(envelope, dict) else None
    text = str(content or "").strip()
    if not text:
        raise RuntimeError("Ollama returned empty content")
    return text


def _find_default_deal_file(project_root: Path, deal_id: str) -> Path:
    direct = project_root / f"deal_{deal_id}.json"
    if direct.exists():
        return direct
    candidates = sorted(
        project_root.glob(f"workspace/deal_analyzer/period_runs/*/deals/deal_{deal_id}.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]
    raise FileNotFoundError(f"deal file not found for deal_id={deal_id}")


def _pick_transcript_text(payload: dict[str, Any]) -> str:
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    transcripts = snapshot.get("transcripts", []) if isinstance(snapshot.get("transcripts"), list) else []
    best = ""
    for item in transcripts:
        if not isinstance(item, dict):
            continue
        txt = str(item.get("transcript_text") or "").strip()
        if len(txt) > len(best):
            best = txt
    if best:
        return best
    analysis = payload.get("analysis", {}) if isinstance(payload.get("analysis"), dict) else {}
    return str(analysis.get("transcript_text_excerpt") or "").strip()


def _build_context(payload: dict[str, Any]) -> dict[str, Any]:
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    crm = snapshot.get("crm", {}) if isinstance(snapshot.get("crm"), dict) else {}
    analysis = payload.get("analysis", {}) if isinstance(payload.get("analysis"), dict) else {}
    transcript = _pick_transcript_text(payload)
    return {
        "deal_id": crm.get("deal_id") or analysis.get("deal_id") or payload.get("deal_id"),
        "deal_name": crm.get("deal_name") or analysis.get("deal_name") or payload.get("deal_name"),
        "manager": crm.get("responsible_user_name") or analysis.get("owner_name"),
        "status": crm.get("status_name") or analysis.get("status_name"),
        "pipeline": crm.get("pipeline_name") or analysis.get("pipeline_name"),
        "product_signals": {
            "product_name": analysis.get("product_name", ""),
            "product_hypothesis": analysis.get("product_hypothesis", ""),
            "product_hypothesis_llm": analysis.get("product_hypothesis_llm", ""),
            "call_signal_product_info": analysis.get("call_signal_product_info", False),
            "call_signal_product_link": analysis.get("call_signal_product_link", False),
            "call_signal_summary_short": analysis.get("call_signal_summary_short", ""),
        },
        "factual_context": {
            "call_evidence_total": (
                snapshot.get("call_evidence", {}).get("summary", {}).get("calls_total", 0)
                if isinstance(snapshot.get("call_evidence"), dict)
                else 0
            ),
            "longest_call_seconds": (
                snapshot.get("call_evidence", {}).get("summary", {}).get("longest_call_duration_seconds", 0)
                if isinstance(snapshot.get("call_evidence"), dict)
                else 0
            ),
            "analysis_confidence": analysis.get("analysis_confidence", ""),
            "risk_flags_top": (analysis.get("risk_flags", []) if isinstance(analysis.get("risk_flags"), list) else [])[:6],
            "notes_count": len(analysis.get("notes_summary_raw", []) if isinstance(analysis.get("notes_summary_raw"), list) else []),
            "tasks_count": len(analysis.get("tasks_summary_raw", []) if isinstance(analysis.get("tasks_summary_raw"), list) else []),
        },
        "transcript_text": transcript,
    }


def _build_messages(context: dict[str, Any]) -> list[dict[str, str]]:
    system = (
        "Ты сильный руководитель отдела продаж и аналитик переговоров. "
        "Пиши по-русски, живо и предметно. Не выдумывай факты, опирайся только на переданный контекст и транскрипт."
    )
    user = (
        "Разбери кейс без JSON-формата.\n\n"
        "Дай короткий структурированный ответ в Markdown с разделами:\n"
        "1) Этапы разговора (что реально произошло)\n"
        "2) Сильные стороны менеджера\n"
        "3) Где недожал / зоны роста\n"
        "4) Какие инструменты/модули дать сотруднику (конкретно)\n"
        "5) На каком этапе менеджер был хорош, а где просел\n\n"
        "Важно:\n"
        "- не уходи в общие слова;\n"
        "- без канцелярита;\n"
        "- если данных не хватает, так и скажи, но предметно;\n"
        "- не подмешивай вымышленные метрики.\n\n"
        f"Фактический контекст:\n{json.dumps(context, ensure_ascii=False, indent=2)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sandbox free-form LLM analysis for one deal artifact.")
    parser.add_argument("--config", default="config/deal_analyzer.local.json")
    parser.add_argument("--deal-id", default="32162059")
    parser.add_argument("--deal-path", default="")
    parser.add_argument("--output-dir", default="workspace/deal_analyzer/sandbox")
    args = parser.parse_args()

    cfg = load_deal_analyzer_config(args.config)
    project_root = cfg.config_path.parent.parent
    deal_path = Path(args.deal_path).resolve() if str(args.deal_path).strip() else _find_default_deal_file(project_root, args.deal_id)
    payload = json.loads(deal_path.read_text(encoding="utf-8-sig"))
    context = _build_context(payload)
    messages = _build_messages(context)

    runtime = resolve_ollama_runtime(cfg=cfg, enabled=True, logger=None, log_prefix="sandbox_free")
    attempts: list[dict[str, Any]] = []
    ordered: list[tuple[str, dict[str, Any]]] = []
    selected = str(runtime.get("selected") or "")
    main_cfg = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
    fb_cfg = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}
    if selected == "fallback":
        ordered.append(("fallback", fb_cfg))
        ordered.append(("main", main_cfg))
    else:
        ordered.append(("main", main_cfg))
        if bool(fb_cfg.get("enabled")):
            ordered.append(("fallback", fb_cfg))

    answer_text = ""
    answered_by = ""
    for source, runtime_cfg in ordered:
        if not runtime_cfg:
            continue
        base_url = str(runtime_cfg.get("base_url") or "").strip()
        model = str(runtime_cfg.get("model") or "").strip()
        timeout_seconds = int(runtime_cfg.get("timeout_seconds") or cfg.ollama_timeout_seconds)
        if not base_url or not model:
            continue
        try:
            text = _chat_text(
                base_url=base_url,
                model=model,
                timeout_seconds=timeout_seconds,
                messages=messages,
            )
            answer_text = text
            answered_by = source
            attempts.append(
                {
                    "source": source,
                    "base_url": base_url,
                    "model": model,
                    "timeout_seconds": timeout_seconds,
                    "ok": True,
                    "error": "",
                }
            )
            break
        except Exception as exc:
            attempts.append(
                {
                    "source": source,
                    "base_url": base_url,
                    "model": model,
                    "timeout_seconds": timeout_seconds,
                    "ok": False,
                    "error": str(exc),
                }
            )

    if not answer_text:
        raise RuntimeError(f"No LLM response from main/fallback. Attempts={attempts}")

    out_dir = (project_root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    md_path = out_dir / f"gemma_free_analysis_{args.deal_id}.md"
    meta_path = out_dir / f"gemma_free_analysis_{args.deal_id}.json"

    md = [
        f"# Sandbox Free Analysis: deal {context.get('deal_id')}",
        "",
        f"- Deal source: `{deal_path}`",
        f"- Answered by: `{answered_by}`",
        f"- Model: `{next((x.get('model') for x in attempts if x.get('ok')), '')}`",
        f"- Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Minimal factual context",
        "```json",
        json.dumps({k: v for k, v in context.items() if k != "transcript_text"}, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Transcript text",
        "```text",
        str(context.get("transcript_text") or ""),
        "```",
        "",
        "## LLM analysis",
        str(answer_text).strip(),
        "",
    ]
    md_path.write_text("\n".join(md), encoding="utf-8")

    meta = {
        "generated_at_utc": ts,
        "deal_source_path": str(deal_path),
        "runtime_selected": runtime.get("selected"),
        "runtime_reason": runtime.get("reason"),
        "answered_by": answered_by,
        "attempts": attempts,
        "context": context,
        "analysis_text": answer_text,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"ok": True, "md_path": str(md_path), "meta_path": str(meta_path), "answered_by": answered_by}, ensure_ascii=False))


if __name__ == "__main__":
    main()

