from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging
from src.safety import ensure_inside_root

from .config import DealAnalyzerConfig, load_deal_analyzer_config
from .exporters import analyzer_output_dir, build_markdown_report, write_analysis_csv, write_json_export, write_markdown_export
from .rules import analyze_deal


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deal analyzer CLI (rule-based MVP)")
    parser.add_argument("--config", required=True, help="Path to analyzer config JSON")
    parser.add_argument("--no-latest", action="store_true", help="Disable latest copy outputs")

    sub = parser.add_subparsers(dest="command", required=True)

    one = sub.add_parser("analyze-deal", help="Analyze one deal from collector JSON")
    one.add_argument("--input", required=True, help="Path to collector deal JSON")

    period = sub.add_parser("analyze-period", help="Analyze period payload from collector JSON")
    period.add_argument("--input", required=True, help="Path to collector period JSON")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_deal_analyzer_config(args.config)
    output_dir = analyzer_output_dir(cfg.output_dir)
    write_latest = not bool(args.no_latest)

    input_path = ensure_inside_root(Path(args.input).resolve(), app.project_root)
    payload = _load_json(input_path)

    if args.command == "analyze-deal":
        _run_analyze_deal(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "analyze-period":
        _run_analyze_period(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def _run_analyze_deal(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized = _extract_single_normalized(payload)
    analysis = analyze_deal(normalized, cfg).to_dict()

    export_payload = {
        "command": "analyze-deal",
        "source": source_name,
        "analysis": analysis,
    }

    json_out = write_json_export(output_dir=output_dir, name="analyze_deal", payload=export_payload, write_latest=write_latest)
    md = build_markdown_report(title="Deal Analyzer / Single Deal", analyses=[analysis])
    md_out = write_markdown_export(output_dir=output_dir, name="analyze_deal", markdown=md, write_latest=write_latest)
    csv_out = write_analysis_csv(output_dir=output_dir, name="analyze_deal", rows=[analysis], write_latest=write_latest)

    logger.info(
        "analyze-deal success: deal_id=%s score=%s json=%s md=%s csv=%s",
        analysis.get("deal_id"),
        analysis.get("score_0_100"),
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
    )


def _run_analyze_period(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized_rows = _extract_period_normalized(payload)
    analyses = [analyze_deal(row, cfg).to_dict() for row in normalized_rows]

    export_payload = {
        "command": "analyze-period",
        "source": source_name,
        "deals_total": len(analyses),
        "analyses": analyses,
    }

    json_out = write_json_export(output_dir=output_dir, name="analyze_period", payload=export_payload, write_latest=write_latest)
    md = build_markdown_report(title="Deal Analyzer / Period", analyses=analyses)
    md_out = write_markdown_export(output_dir=output_dir, name="analyze_period", markdown=md, write_latest=write_latest)
    csv_out = write_analysis_csv(output_dir=output_dir, name="analyze_period", rows=analyses, write_latest=write_latest)

    logger.info(
        "analyze-period success: deals=%s json=%s md=%s csv=%s",
        len(analyses),
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
    )


def _extract_single_normalized(payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        if isinstance(payload.get("normalized"), dict):
            return payload["normalized"]
        if isinstance(payload.get("analysis"), dict):
            return payload["analysis"]
        if _looks_like_normalized_row(payload):
            return payload
    raise RuntimeError("analyze-deal input does not contain normalized deal payload")


def _extract_period_normalized(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("normalized_deals")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
        rows = payload.get("analyses")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    raise RuntimeError("analyze-period input does not contain normalized deals list")


def _looks_like_normalized_row(payload: dict[str, Any]) -> bool:
    return any(key in payload for key in ("deal_id", "amo_lead_id", "deal_name", "presentation_detected"))


def _load_json(path: Path) -> dict[str, Any] | list[Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, (dict, list)):
        raise RuntimeError(f"Unsupported JSON root in input: {path}")
    return data


if __name__ == "__main__":
    main()
