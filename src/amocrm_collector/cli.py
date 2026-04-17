from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.config import load_config
from src.logger import setup_logging

from .client import AmoCollectorClient
from .config import AmoCollectorConfig, load_collector_config
from .exporters import collector_output_dir, write_json_export, write_normalized_csv, write_normalized_jsonl
from .normalizer import AmoDealNormalizer


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="amoCRM collector CLI (read-only)")
    parser.add_argument("--config", required=True, help="Path to collector config JSON")
    parser.add_argument("--no-latest", action="store_true", help="Disable latest copy outputs")

    sub = parser.add_subparsers(dest="command", required=True)

    period = sub.add_parser("collect-period", help="Collect deals for date range")
    period.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    period.add_argument("--date-to", required=True, help="YYYY-MM-DD")

    deal = sub.add_parser("collect-deal", help="Collect one deal bundle")
    deal.add_argument("--deal-id", type=int, required=True)

    sub.add_parser("schema-check", help="Export account schema snapshot")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_collector_config(args.config)
    auth_cfg = load_amocrm_auth_config(str(cfg.auth_config_path))
    state = load_auth_state(auth_cfg.state_path)

    base_domain = (cfg.base_domain or state.base_domain or auth_cfg.base_domain).strip()
    access_token = str(state.access_token or "").strip()
    if not base_domain:
        raise RuntimeError("collector: base_domain is empty (collector config/auth state)")
    if not access_token:
        raise RuntimeError(f"collector: no access token found in auth state: {auth_cfg.state_path}")

    output_dir = collector_output_dir(cfg.output_dir)
    client = AmoCollectorClient(base_domain=base_domain, access_token=access_token)
    normalizer = AmoDealNormalizer(cfg)
    write_latest = not bool(args.no_latest)

    if args.command == "schema-check":
        _run_schema_check(client, output_dir, base_domain, write_latest, logger)
        return

    if args.command == "collect-deal":
        _run_collect_deal(client, normalizer, output_dir, base_domain, int(args.deal_id), write_latest, logger)
        return

    if args.command == "collect-period":
        _run_collect_period(client, normalizer, cfg, output_dir, base_domain, args.date_from, args.date_to, write_latest, logger)
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def _run_schema_check(client: AmoCollectorClient, output_dir, base_domain: str, write_latest: bool, logger) -> None:
    account = client.get_account()
    users = client.get_users_cache()
    pipelines = client.get_pipelines_cache()
    deal_fields = client.get_custom_fields("leads")
    contact_fields = client.get_custom_fields("contacts")
    company_fields = client.get_custom_fields("companies")

    payload: dict[str, Any] = {
        "command": "schema-check",
        "base_domain": base_domain,
        "counts": {
            "users": len(users),
            "pipelines": len(pipelines),
            "deal_custom_fields": len(deal_fields),
            "contact_custom_fields": len(contact_fields),
            "company_custom_fields": len(company_fields),
        },
        "account": account,
        "users": list(users.values()),
        "pipelines": pipelines,
        "custom_fields": {
            "deals": deal_fields,
            "contacts": contact_fields,
            "companies": company_fields,
        },
    }
    exported = write_json_export(output_dir=output_dir, name="schema_check", payload=payload, write_latest=write_latest)
    logger.info(
        "schema check exported: users=%s pipelines=%s deal_fields=%s contact_fields=%s company_fields=%s path=%s latest=%s",
        payload["counts"]["users"],
        payload["counts"]["pipelines"],
        payload["counts"]["deal_custom_fields"],
        payload["counts"]["contact_custom_fields"],
        payload["counts"]["company_custom_fields"],
        exported.timestamped,
        exported.latest,
    )


def _run_collect_deal(
    client: AmoCollectorClient,
    normalizer: AmoDealNormalizer,
    output_dir,
    base_domain: str,
    deal_id: int,
    write_latest: bool,
    logger,
) -> None:
    bundle = client.collect_lead_bundle(deal_id)
    normalized = normalizer.normalize_bundle(bundle)
    payload = {
        "command": "collect-deal",
        "base_domain": base_domain,
        "deal_id": deal_id,
        "normalized": normalized,
        "raw_bundle": bundle,
    }
    exported = write_json_export(output_dir=output_dir, name=f"deal_{deal_id}", payload=payload, write_latest=write_latest)
    logger.info(
        "collect-deal success: deal_id=%s manager_scope_allowed=%s presentation_detected=%s path=%s latest=%s",
        deal_id,
        normalized.get("manager_scope_allowed"),
        normalized.get("presentation_detected"),
        exported.timestamped,
        exported.latest,
    )


def _run_collect_period(
    client: AmoCollectorClient,
    normalizer: AmoDealNormalizer,
    cfg: AmoCollectorConfig,
    output_dir,
    base_domain: str,
    date_from: str,
    date_to: str,
    write_latest: bool,
    logger,
) -> None:
    unix_from, unix_to = _parse_date_range(date_from, date_to)

    raw_deals: list[dict[str, Any]] = []
    page = 1
    while True:
        batch = client.get_leads_by_period(
            date_from_unix=unix_from,
            date_to_unix=unix_to,
            page=page,
            limit=250,
            pipeline_ids=cfg.pipeline_ids_include or None,
        )
        if not batch:
            break
        raw_deals.extend(batch)
        if len(batch) < 250:
            break
        page += 1

    normalized_rows: list[dict[str, Any]] = []
    skipped_by_scope = 0
    for deal_short in raw_deals:
        deal_id = deal_short.get("id")
        if not isinstance(deal_id, int):
            continue
        bundle = client.collect_lead_bundle(deal_id)
        normalized = normalizer.normalize_bundle(bundle)
        if not bool(normalized.get("manager_scope_allowed", False)):
            skipped_by_scope += 1
            continue
        normalized_rows.append(normalized)

    payload = {
        "command": "collect-period",
        "base_domain": base_domain,
        "date_from": date_from,
        "date_to": date_to,
        "counts": {
            "raw_deals": len(raw_deals),
            "normalized": len(normalized_rows),
            "skipped_by_scope": skipped_by_scope,
        },
        "normalized_deals": normalized_rows,
    }

    name = f"collect_period_{date_from}_{date_to}"
    main_export = write_json_export(output_dir=output_dir, name=name, payload=payload, write_latest=write_latest)
    jsonl_export = write_normalized_jsonl(output_dir=output_dir, name=name, rows=normalized_rows, write_latest=write_latest)
    csv_export = write_normalized_csv(output_dir=output_dir, name=name, rows=normalized_rows, write_latest=write_latest)

    logger.info(
        "collect-period success: raw_deals=%s normalized=%s skipped_by_scope=%s json=%s jsonl=%s csv=%s",
        len(raw_deals),
        len(normalized_rows),
        skipped_by_scope,
        main_export.timestamped,
        jsonl_export.timestamped,
        csv_export.timestamped,
    )


def _parse_date_range(date_from: str, date_to: str) -> tuple[int, int]:
    dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt_to = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if dt_to < dt_from:
        raise RuntimeError("collect-period: date-to must be >= date-from")

    end = dt_to + timedelta(days=1) - timedelta(seconds=1)
    return int(dt_from.timestamp()), int(end.timestamp())


if __name__ == "__main__":
    main()
