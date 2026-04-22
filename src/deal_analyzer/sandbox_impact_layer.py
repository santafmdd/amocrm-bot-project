from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_deal_analyzer_config
from .llm_runtime import resolve_ollama_runtime
from .sandbox_free_analysis import _chat_text


DEFAULT_FUNNEL = {
    "Набор->Дозвон": 0.28,
    "Дозвон->ЛПР": 0.36,
    "ЛПР->Встреча": 0.42,
    "Встреча->Демо": 0.55,
    "Демо->Тест": 0.40,
    "Тест->Оплата": 0.33,
}


def _extract_free_analysis(md_text: str) -> str:
    marker = "## LLM analysis"
    idx = md_text.find(marker)
    if idx == -1:
        return md_text.strip()
    return md_text[idx + len(marker) :].strip()


def _build_messages(*, free_analysis_text: str, funnel: dict[str, float], deal_id: str) -> list[dict[str, str]]:
    system = (
        "Ты руководитель отдела продаж. "
        "Тебе уже дали готовый разбор разговора. "
        "Не пересобирай разговор заново: дострой только управленческий смысл."
    )
    user = (
        f"Кейс: deal_id={deal_id}\n\n"
        "Ниже готовый свободный разбор звонка. Не оспаривай его и не пересказывай дословно.\n"
        "Нужно выдать второй слой: управленческий смысл для сотрудника и руководителя.\n\n"
        "Сделай markdown с разделами:\n"
        "1) Почему сотруднику выгодно закрыть эти зоны роста (мотивация без воды)\n"
        "2) Ожидаемый эффект — количество (в штуках за неделю, можно дроби)\n"
        "3) Ожидаемый эффект — качество (гипотеза по этапам/конверсиям)\n"
        "4) Влияние на основной проблемный этап\n"
        "5) Каскадное влияние на нижние этапы\n"
        "6) 3-5 управленческих акцентов, что говорить сотруднику на разборе\n\n"
        "Ограничения:\n"
        "- Не пиши JSON.\n"
        "- Не используй канцелярит.\n"
        "- Не обещай космический рост.\n"
        "- Для количества используй только штуки/диапазоны в неделю.\n"
        "- Для качества можно писать гипотезу через этапы.\n\n"
        "Упрощенная воронка (для гипотезы каскада):\n"
        f"{json.dumps(funnel, ensure_ascii=False, indent=2)}\n\n"
        "Готовый разбор (вход):\n"
        f"{free_analysis_text}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sandbox: second-layer management impact from ready free analysis.")
    parser.add_argument("--config", default="config/deal_analyzer.local.json")
    parser.add_argument("--deal-id", default="32162059")
    parser.add_argument(
        "--input-markdown",
        default="workspace/deal_analyzer/sandbox/gemma_free_analysis_32162059.md",
        help="Path to ready free analysis markdown from Prompt 3",
    )
    parser.add_argument(
        "--funnel-json",
        default="",
        help="Optional JSON object with stage->conversion. If empty, DEFAULT_FUNNEL is used.",
    )
    parser.add_argument("--output-dir", default="workspace/deal_analyzer/sandbox")
    args = parser.parse_args()

    cfg = load_deal_analyzer_config(args.config)
    project_root = cfg.config_path.parent.parent
    input_md_path = (project_root / args.input_markdown).resolve()
    if not input_md_path.exists():
        raise FileNotFoundError(f"input markdown not found: {input_md_path}")

    md_text = input_md_path.read_text(encoding="utf-8")
    free_analysis_text = _extract_free_analysis(md_text)
    funnel = DEFAULT_FUNNEL
    if str(args.funnel_json).strip():
        maybe = json.loads(args.funnel_json)
        if isinstance(maybe, dict) and maybe:
            funnel = {str(k): float(v) for k, v in maybe.items()}

    runtime = resolve_ollama_runtime(cfg=cfg, enabled=True, logger=None, log_prefix="sandbox_impact")
    selected = str(runtime.get("selected") or "")
    main_cfg = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
    fb_cfg = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}
    order: list[tuple[str, dict[str, Any]]] = []
    if selected == "fallback":
        order.append(("fallback", fb_cfg))
        order.append(("main", main_cfg))
    else:
        order.append(("main", main_cfg))
        if bool(fb_cfg.get("enabled")):
            order.append(("fallback", fb_cfg))

    attempts: list[dict[str, Any]] = []
    answer = ""
    answered_by = ""
    used_model = ""
    messages = _build_messages(free_analysis_text=free_analysis_text, funnel=funnel, deal_id=str(args.deal_id))
    for source, rcfg in order:
        if not rcfg:
            continue
        base_url = str(rcfg.get("base_url") or "").strip()
        model = str(rcfg.get("model") or "").strip()
        timeout_seconds = int(rcfg.get("timeout_seconds") or cfg.ollama_timeout_seconds)
        if not base_url or not model:
            continue
        try:
            out = _chat_text(
                base_url=base_url,
                model=model,
                timeout_seconds=timeout_seconds,
                messages=messages,
            )
            answer = out.strip()
            answered_by = source
            used_model = model
            attempts.append(
                {
                    "source": source,
                    "model": model,
                    "base_url": base_url,
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
                    "model": model,
                    "base_url": base_url,
                    "timeout_seconds": timeout_seconds,
                    "ok": False,
                    "error": str(exc),
                }
            )
    if not answer:
        raise RuntimeError(f"No response from main/fallback. Attempts={attempts}")

    out_dir = (project_root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_out = out_dir / f"gemma_impact_layer_{args.deal_id}.md"
    meta_out = out_dir / f"gemma_impact_layer_{args.deal_id}.json"

    render = [
        f"# Sandbox Impact Layer: deal {args.deal_id}",
        "",
        f"- Input free analysis: `{input_md_path}`",
        f"- Answered by: `{answered_by}`",
        f"- Model: `{used_model}`",
        f"- Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Output",
        answer,
        "",
    ]
    md_out.write_text("\n".join(render), encoding="utf-8")
    meta = {
        "deal_id": str(args.deal_id),
        "input_markdown": str(input_md_path),
        "answered_by": answered_by,
        "used_model": used_model,
        "attempts": attempts,
        "runtime_selected": runtime.get("selected"),
        "runtime_reason": runtime.get("reason"),
        "funnel_used": funnel,
        "output_markdown": str(md_out),
    }
    meta_out.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output_md": str(md_out), "output_meta": str(meta_out), "answered_by": answered_by, "model": used_model}, ensure_ascii=False))


if __name__ == "__main__":
    main()

