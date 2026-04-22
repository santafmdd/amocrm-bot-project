from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_BLOCK_KEYS = [
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


def _extract_blocks(payload: dict[str, Any]) -> dict[str, str]:
    raw = {}
    if isinstance(payload.get("styled_blocks"), dict):
        raw = payload["styled_blocks"]
    elif isinstance(payload.get("blocks"), dict):
        raw = payload["blocks"]
    blocks: dict[str, str] = {}
    for key in REQUIRED_BLOCK_KEYS:
        blocks[key] = str(raw.get(key) or "").strip()
    return blocks


def _missing_keys(blocks: dict[str, str]) -> list[str]:
    return [k for k in REQUIRED_BLOCK_KEYS if not str(blocks.get(k) or "").strip()]


def _build_writer_payload(*, deal_id: str, blocks: dict[str, str]) -> dict[str, Any]:
    return {
        "deal_id": str(deal_id),
        "source_of_truth": "styled_blocks",
        "assembler_only": True,
        "blocks": {k: str(blocks.get(k) or "").strip() for k in REQUIRED_BLOCK_KEYS},
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Final assembler: validates ready styled blocks and builds writer-ready JSON only."
    )
    parser.add_argument(
        "--input-json",
        default="workspace/deal_analyzer/sandbox/gemma_styled_blocks_32162059.json",
    )
    parser.add_argument("--deal-id", default="32162059")
    parser.add_argument("--output-dir", default="workspace/deal_analyzer/sandbox")
    parser.add_argument(
        "--on-missing",
        choices=("fail", "dry-run"),
        default="fail",
        help="If required blocks are missing: fail hard (default) or emit dry-run status artifact.",
    )
    args = parser.parse_args()

    project_root = Path.cwd()
    input_path = (project_root / args.input_json).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"styled blocks artifact not found: {input_path}")

    payload = json.loads(_read_text_safe(input_path))
    blocks = _extract_blocks(payload)
    missing = _missing_keys(blocks)

    out_dir = (project_root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"writer_final_payload_{args.deal_id}.json"

    if missing:
        if args.on_missing == "fail":
            raise RuntimeError(f"missing required styled blocks: {', '.join(missing)}")
        dry = {
            "ok": False,
            "status": "dry_run_missing_required_blocks",
            "deal_id": str(args.deal_id),
            "source_of_truth": "styled_blocks",
            "assembler_only": True,
            "missing_required_blocks": missing,
            "input_json": str(input_path),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        out_path.write_text(json.dumps(dry, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok": False, "mode": "dry_run", "output_json": str(out_path), "missing_required_blocks": missing}, ensure_ascii=False))
        return

    final_payload = {
        "ok": True,
        "status": "assembled",
        "deal_id": str(args.deal_id),
        "source_of_truth": "styled_blocks",
        "assembler_only": True,
        "input_json": str(input_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "writer_payload": _build_writer_payload(deal_id=str(args.deal_id), blocks=blocks),
    }
    out_path.write_text(json.dumps(final_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "output_json": str(out_path), "source_of_truth": "styled_blocks", "assembler_only": True}, ensure_ascii=False))


if __name__ == "__main__":
    main()
