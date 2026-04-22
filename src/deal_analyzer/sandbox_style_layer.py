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


def _read_text_safe(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def _collect_style_sources(project_root: Path, max_chars_per_file: int = 3000) -> dict[str, str]:
    out: dict[str, str] = {}
    pattern_file = project_root / "docs" / "мой паттерн общения.txt"
    if pattern_file.exists():
        out[str(pattern_file)] = _read_text_safe(pattern_file)[:max_chars_per_file].strip()
    style_root = project_root / "docs" / "style_sources"
    if style_root.exists():
        files = sorted(
            [p for p in style_root.rglob("*") if p.is_file() and p.suffix.lower() in {".txt", ".md", ".html"}]
        )
        for p in files[:80]:
            out[str(p)] = _read_text_safe(p)[:max_chars_per_file].strip()
    return {k: v for k, v in out.items() if v}


def _blocks_to_markdown(blocks: dict[str, str]) -> str:
    lines: list[str] = []
    for key in BLOCK_KEYS:
        lines.append(f"### {key}")
        lines.append(str(blocks.get(key) or "").strip())
        lines.append("")
    return "\n".join(lines).strip()


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


def _build_messages(*, blocks: dict[str, str], style_sources: dict[str, str]) -> list[dict[str, str]]:
    source_preview_parts: list[str] = []
    for path, text in list(style_sources.items())[:14]:
        source_preview_parts.append(f"[source] {path}\n{text[:1200]}")
    source_preview = "\n\n".join(source_preview_parts)

    system = (
        "Ты редактор управленческих текстов. "
        "Твоя задача: перефразировать текст под живой рабочий лексикон Артура, "
        "не меняя смысл, факты, структуру блоков и фокус."
    )
    user = (
        "Ниже блоки уже разобранного кейса. Не анализируй кейс заново.\n"
        "Сделай только style-rewrite.\n\n"
        "Жесткие правила:\n"
        "- оставить те же 9 блоков и те же заголовки `### key`;\n"
        "- не добавлять новых сущностей/фактов/чисел;\n"
        "- не уводить текст в CRM-hygiene по умолчанию;\n"
        "- не менять приоритеты и смысл акцентов;\n"
        "- можно только улучшить речь: плотнее, живее, понятнее руководителю.\n\n"
        "Вывод: только markdown с 9 блоками, без JSON и без пояснений.\n\n"
        "Style reference sources:\n"
        f"{source_preview}\n\n"
        "Входные блоки:\n"
        f"{_blocks_to_markdown(blocks)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _runtime_order(runtime: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
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
    return order


def main() -> None:
    parser = argparse.ArgumentParser(description="Sandbox style-layer over already split blocks.")
    parser.add_argument("--config", default="config/deal_analyzer.local.json")
    parser.add_argument("--deal-id", default="32162059")
    parser.add_argument("--input-json", default="workspace/deal_analyzer/sandbox/gemma_block_split_32162059.json")
    parser.add_argument("--output-dir", default="workspace/deal_analyzer/sandbox")
    args = parser.parse_args()

    cfg = load_deal_analyzer_config(args.config)
    project_root = cfg.config_path.parent.parent
    input_path = (project_root / args.input_json).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"input block split json not found: {input_path}")
    payload = json.loads(_read_text_safe(input_path))
    input_blocks = payload.get("blocks", {}) if isinstance(payload.get("blocks"), dict) else {}
    for key in BLOCK_KEYS:
        input_blocks.setdefault(key, "")

    style_sources = _collect_style_sources(project_root)
    runtime = resolve_ollama_runtime(cfg=cfg, enabled=True, logger=None, log_prefix="sandbox_style")
    attempts: list[dict[str, Any]] = []
    styled_md = ""
    answered_by = ""
    used_model = ""
    messages = _build_messages(blocks=input_blocks, style_sources=style_sources)
    for source, rcfg in _runtime_order(runtime):
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
            styled_md = out.strip()
            answered_by = source
            used_model = model
            attempts.append({"source": source, "model": model, "ok": True, "error": ""})
            break
        except Exception as exc:
            attempts.append({"source": source, "model": model, "ok": False, "error": str(exc)})
    if not styled_md:
        raise RuntimeError(f"No response from main/fallback. Attempts={attempts}")

    styled_blocks = _parse_blocks(styled_md)
    missing = [k for k in BLOCK_KEYS if not str(styled_blocks.get(k) or "").strip()]
    if missing:
        # keep structure strict: fallback to original for missing blocks
        for k in missing:
            styled_blocks[k] = str(input_blocks.get(k) or "").strip()
    final_md = _blocks_to_markdown(styled_blocks)

    out_dir = (project_root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_out = out_dir / f"gemma_styled_blocks_{args.deal_id}.md"
    json_out = out_dir / f"gemma_styled_blocks_{args.deal_id}.json"

    render = [
        f"# Sandbox Styled Blocks: deal {args.deal_id}",
        "",
        f"- Input blocks: `{input_path}`",
        f"- Answered by: `{answered_by}`",
        f"- Model: `{used_model}`",
        f"- Style sources loaded: `{len(style_sources)}`",
        f"- Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Styled output",
        final_md,
        "",
    ]
    md_out.write_text("\n".join(render), encoding="utf-8")
    meta = {
        "deal_id": str(args.deal_id),
        "input_json": str(input_path),
        "answered_by": answered_by,
        "model": used_model,
        "runtime_selected": runtime.get("selected"),
        "runtime_reason": runtime.get("reason"),
        "style_sources_loaded": list(style_sources.keys()),
        "attempts": attempts,
        "original_blocks": input_blocks,
        "styled_blocks": styled_blocks,
    }
    json_out.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output_md": str(md_out), "output_json": str(json_out), "model": used_model, "answered_by": answered_by, "style_sources_loaded": len(style_sources)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

