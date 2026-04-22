from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_deal_analyzer_config
from .llm_runtime import resolve_ollama_runtime
from .sandbox_free_analysis import _chat_text
from .sandbox_impact_layer import _extract_free_analysis

BLOCK_KEYS = [
    "key_takeaway",
    "strong_sides",
    "growth_zones",
    "why_important",
    "reinforce",
    "fix_action",
    "coaching_list",
    "expected_quantity",
    "expected_quality",
]


def _extract_output(md_text: str) -> str:
    marker = "## Output"
    idx = md_text.find(marker)
    if idx == -1:
        return md_text.strip()
    return md_text[idx + len(marker) :].strip()


def _detect_case_mode(*, free_text: str, impact_text: str, explicit: str) -> str:
    if explicit in {"negotiation", "secretary", "redial"}:
        return explicit
    text = f"{free_text}\n{impact_text}".lower()
    if any(x in text for x in ("секретар", "ресепш", "на почту", "соедините", "маршрутиз")):
        return "secretary"
    if any(x in text for x in ("недозвон", "перезвон", "voicemail", "автоответ", "не может ответить")):
        return "redial"
    return "negotiation"


def _banned_topics_for_mode(mode: str) -> tuple[str, ...]:
    if mode == "secretary":
        return ("бриф", "презентац", "демо", "демонстрац", "боль клиента", "бизнес-задач")
    if mode == "redial":
        return ("боль клиента", "бизнес-задач", "бриф", "демо", "демонстрац", "презентац")
    return ()


def _build_messages(*, deal_id: str, mode: str, free_text: str, impact_text: str, banned_topics: tuple[str, ...]) -> list[dict[str, str]]:
    system = (
        "Ты sales-аналитик. Твоя задача: не анализировать разговор заново, а только распределить уже готовый смысл по блокам."
    )
    user = (
        f"Кейс deal_id={deal_id}, режим={mode}.\n\n"
        "Ниже два уже готовых слоя:\n"
        "1) свободный разбор переговоров\n"
        "2) управленческий слой мотивации/эффекта\n\n"
        "Сделай block split в markdown по 9 блокам, строго с такими заголовками:\n"
        "### key_takeaway\n"
        "### strong_sides\n"
        "### growth_zones\n"
        "### why_important\n"
        "### reinforce\n"
        "### fix_action\n"
        "### coaching_list\n"
        "### expected_quantity\n"
        "### expected_quality\n\n"
        "Требования:\n"
        "- не писать JSON;\n"
        "- не пересобирать диалог заново;\n"
        "- использовать только смысл из входных двух слоев;\n"
        "- expected_quantity: в штуках за неделю, можно дроби;\n"
        "- expected_quality: гипотеза по качеству этапа и соседних этапов;\n"
        "- без канцелярита.\n"
    )
    if banned_topics:
        user += (
            "\nЖесткий запрет на темы для этого режима (не использовать ни в одном блоке):\n- "
            + "\n- ".join(banned_topics)
            + "\n"
        )
    user += (
        "\nВход 1 (free analysis):\n"
        f"{free_text}\n\n"
        "Вход 2 (impact layer):\n"
        f"{impact_text}\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _parse_blocks(markdown: str) -> dict[str, str]:
    lines = markdown.splitlines()
    result: dict[str, list[str]] = {k: [] for k in BLOCK_KEYS}
    current: str | None = None
    header_re = re.compile(r"^\s*###\s+([a-z_]+)\s*$")
    for ln in lines:
        m = header_re.match(ln.strip())
        if m:
            key = m.group(1).strip()
            current = key if key in result else None
            continue
        if current is not None:
            result[current].append(ln)
    return {k: "\n".join(v).strip() for k, v in result.items()}


def _contains_banned(blocks: dict[str, str], banned_topics: tuple[str, ...]) -> list[str]:
    if not banned_topics:
        return []
    text = "\n".join(str(blocks.get(k) or "") for k in BLOCK_KEYS).lower()
    found: list[str] = []
    for marker in banned_topics:
        if marker.lower() in text:
            found.append(marker)
    return sorted(set(found))


def _repair_messages(*, original_markdown: str, found_banned: list[str], mode: str) -> list[dict[str, str]]:
    system = "Ты редактор sales-текста. Убери запрещенные темы, сохрани смысл block split."
    user = (
        f"Режим={mode}. Ниже block split, в нем есть запрещенные темы: {', '.join(found_banned)}.\n"
        "Перепиши block split, сохрани 9 заголовков ### <key> и смысл, но полностью убери запрещенные темы.\n\n"
        f"{original_markdown}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sandbox block split layer from free analysis + impact layer.")
    parser.add_argument("--config", default="config/deal_analyzer.local.json")
    parser.add_argument("--deal-id", default="32162059")
    parser.add_argument("--free-markdown", default="workspace/deal_analyzer/sandbox/gemma_free_analysis_32162059.md")
    parser.add_argument("--impact-markdown", default="workspace/deal_analyzer/sandbox/gemma_impact_layer_32162059.md")
    parser.add_argument("--case-mode", default="auto", choices=["auto", "negotiation", "secretary", "redial"])
    parser.add_argument("--output-dir", default="workspace/deal_analyzer/sandbox")
    args = parser.parse_args()

    cfg = load_deal_analyzer_config(args.config)
    project_root = cfg.config_path.parent.parent
    free_path = (project_root / args.free_markdown).resolve()
    impact_path = (project_root / args.impact_markdown).resolve()
    if not free_path.exists():
        raise FileNotFoundError(f"free analysis markdown not found: {free_path}")
    if not impact_path.exists():
        raise FileNotFoundError(f"impact markdown not found: {impact_path}")

    free_raw = free_path.read_text(encoding="utf-8")
    impact_raw = impact_path.read_text(encoding="utf-8")
    free_text = _extract_free_analysis(free_raw)
    impact_text = _extract_output(impact_raw)
    mode = _detect_case_mode(free_text=free_text, impact_text=impact_text, explicit=str(args.case_mode))
    banned_topics = _banned_topics_for_mode(mode)

    runtime = resolve_ollama_runtime(cfg=cfg, enabled=True, logger=None, log_prefix="sandbox_block_split")
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
    output_md = ""
    answered_by = ""
    used_model = ""
    prompt_messages = _build_messages(
        deal_id=str(args.deal_id),
        mode=mode,
        free_text=free_text,
        impact_text=impact_text,
        banned_topics=banned_topics,
    )

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
                messages=prompt_messages,
            )
            output_md = out.strip()
            answered_by = source
            used_model = model
            attempts.append({"source": source, "model": model, "ok": True, "error": ""})
            break
        except Exception as exc:
            attempts.append({"source": source, "model": model, "ok": False, "error": str(exc)})

    if not output_md:
        raise RuntimeError(f"No response from main/fallback. Attempts={attempts}")

    blocks = _parse_blocks(output_md)
    found_banned = _contains_banned(blocks, banned_topics)
    repaired = False
    if found_banned:
        repair_msgs = _repair_messages(original_markdown=output_md, found_banned=found_banned, mode=mode)
        # repair on same model that answered successfully
        rcfg = next((cfg_item for src, cfg_item in order if src == answered_by), {})
        if rcfg:
            repaired_out = _chat_text(
                base_url=str(rcfg.get("base_url") or "").strip(),
                model=str(rcfg.get("model") or "").strip(),
                timeout_seconds=int(rcfg.get("timeout_seconds") or cfg.ollama_timeout_seconds),
                messages=repair_msgs,
            )
            output_md = repaired_out.strip()
            blocks = _parse_blocks(output_md)
            found_banned = _contains_banned(blocks, banned_topics)
            repaired = True

    out_dir = (project_root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_out = out_dir / f"gemma_block_split_{args.deal_id}.md"
    json_out = out_dir / f"gemma_block_split_{args.deal_id}.json"

    render = [
        f"# Sandbox Block Split: deal {args.deal_id}",
        "",
        f"- Free analysis input: `{free_path}`",
        f"- Impact layer input: `{impact_path}`",
        f"- Case mode: `{mode}`",
        f"- Answered by: `{answered_by}`",
        f"- Model: `{used_model}`",
        f"- Banned topics: `{', '.join(banned_topics) if banned_topics else 'none'}`",
        f"- Banned topics found after generation: `{', '.join(found_banned) if found_banned else 'none'}`",
        f"- Repaired once: `{repaired}`",
        f"- Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Block split output",
        output_md,
        "",
    ]
    md_out.write_text("\n".join(render), encoding="utf-8")
    payload = {
        "deal_id": str(args.deal_id),
        "case_mode": mode,
        "answered_by": answered_by,
        "model": used_model,
        "banned_topics": list(banned_topics),
        "banned_topics_found_after_generation": found_banned,
        "repaired_once": repaired,
        "blocks": blocks,
        "inputs": {
            "free_markdown": str(free_path),
            "impact_markdown": str(impact_path),
        },
        "attempts": attempts,
    }
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output_md": str(md_out), "output_json": str(json_out), "case_mode": mode, "answered_by": answered_by, "model": used_model}, ensure_ascii=False))


if __name__ == "__main__":
    main()

