from __future__ import annotations

import argparse
from typing import Any

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.oauth_client import AmoOAuthClient
from src.amocrm_auth.state_store import build_state_from_token_response, load_auth_state, save_auth_state
from src.config import load_config
from src.logger import setup_logging

from .client import AmoDiscoveryClient
from .exporters import discovery_output_dir, write_export


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="amoCRM discovery CLI")
    parser.add_argument("--config", default="", help="Path to amocrm auth config JSON")
    parser.add_argument("--base-domain", default="", help="Optional base domain override")
    parser.add_argument("--refresh-if-needed", action="store_true", help="Try refresh token when access token expired")
    parser.add_argument("--no-latest", action="store_true", help="Do not create *_latest.json copy")

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("account", help="Export account snapshot")
    sub.add_parser("users", help="Export users list")
    sub.add_parser("pipelines", help="Export leads pipelines and statuses")
    sub.add_parser("lead-fields", help="Export leads custom fields")
    sub.add_parser("contact-fields", help="Export contacts custom fields")
    sub.add_parser("company-fields", help="Export companies custom fields")

    lead_bundle = sub.add_parser("lead-bundle", help="Export sample lead bundle")
    lead_bundle.add_argument("--lead-id", type=int, required=True, help="Lead ID")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_amocrm_auth_config(args.config or None)
    state = load_auth_state(cfg.state_path)

    base_domain = (args.base_domain or state.base_domain or cfg.base_domain).strip()
    if not base_domain:
        raise RuntimeError("Base domain is empty. Provide --base-domain or configure AMOCRM_BASE_DOMAIN")
    if not state.access_token.strip():
        raise RuntimeError(f"No access token found in state file: {cfg.state_path}")

    access_token = state.access_token
    if args.refresh_if_needed and state.is_expired() and state.refresh_token:
        oauth = AmoOAuthClient(cfg)
        logger.info("amoCRM discovery: access token expired, attempting refresh")
        token_payload = oauth.refresh_access_token(refresh_token=state.refresh_token, base_domain=base_domain)
        state = build_state_from_token_response(
            base_domain=base_domain,
            token_payload=token_payload,
            code=state.last_code,
            state=state.last_state,
            referer=state.last_referer,
        )
        save_auth_state(cfg.state_path, state)
        access_token = state.access_token
        logger.info("amoCRM discovery: refreshed token saved to %s", cfg.state_path)

    client = AmoDiscoveryClient(base_domain=base_domain, access_token=access_token)
    output_dir = discovery_output_dir()
    write_latest = not bool(args.no_latest)

    command = str(args.command)
    if command == "account":
        account = client.get_account_snapshot()
        payload = {
            "command": "account",
            "base_domain": base_domain,
            "account": account,
        }
        paths = write_export(output_dir=output_dir, name="account_snapshot", payload=payload, write_latest=write_latest)
        logger.info("account snapshot exported: path=%s latest=%s", paths.timestamped, paths.latest)
        return

    if command == "users":
        users = client.get_users(limit=250)
        payload = {
            "command": "users",
            "base_domain": base_domain,
            "count": len(users),
            "users": users,
        }
        paths = write_export(output_dir=output_dir, name="users", payload=payload, write_latest=write_latest)
        logger.info("users export success: count=%s path=%s latest=%s", len(users), paths.timestamped, paths.latest)
        return

    if command == "pipelines":
        pipelines = client.get_pipelines_with_statuses(limit=250)
        payload = {
            "command": "pipelines",
            "base_domain": base_domain,
            "count": len(pipelines),
            "pipelines": pipelines,
        }
        paths = write_export(output_dir=output_dir, name="pipelines_statuses", payload=payload, write_latest=write_latest)
        logger.info("pipelines export success: count=%s path=%s latest=%s", len(pipelines), paths.timestamped, paths.latest)
        return

    if command == "lead-fields":
        _export_custom_fields(client, output_dir, base_domain, "leads", "lead_custom_fields", write_latest, logger)
        return

    if command == "contact-fields":
        _export_custom_fields(client, output_dir, base_domain, "contacts", "contact_custom_fields", write_latest, logger)
        return

    if command == "company-fields":
        _export_custom_fields(client, output_dir, base_domain, "companies", "company_custom_fields", write_latest, logger)
        return

    if command == "lead-bundle":
        lead_id = int(args.lead_id)
        bundle = client.build_lead_bundle(lead_id)
        payload = {
            "command": "lead-bundle",
            "base_domain": base_domain,
            "lead_id": lead_id,
            "bundle": bundle,
            "counts": {
                "contacts": len(_as_list(bundle.get("contacts"))),
                "companies": len(_as_list(bundle.get("companies"))),
                "notes": len(_as_list(bundle.get("notes"))),
                "tasks": len(_as_list(bundle.get("tasks"))),
                "tags": len(_as_list(bundle.get("tags"))),
            },
        }
        paths = write_export(output_dir=output_dir, name=f"lead_bundle_{lead_id}", payload=payload, write_latest=write_latest)
        logger.info(
            "lead bundle export success: lead_id=%s contacts=%s companies=%s notes=%s tasks=%s tags=%s path=%s latest=%s",
            lead_id,
            payload["counts"]["contacts"],
            payload["counts"]["companies"],
            payload["counts"]["notes"],
            payload["counts"]["tasks"],
            payload["counts"]["tags"],
            paths.timestamped,
            paths.latest,
        )
        return

    raise RuntimeError(f"Unsupported command: {command}")


def _export_custom_fields(
    client: AmoDiscoveryClient,
    output_dir,
    base_domain: str,
    entity: str,
    export_name: str,
    write_latest: bool,
    logger,
) -> None:
    fields = client.get_custom_fields(entity=entity, limit=250)
    payload: dict[str, Any] = {
        "command": f"{entity}-fields",
        "base_domain": base_domain,
        "entity": entity,
        "count": len(fields),
        "custom_fields": fields,
    }
    paths = write_export(output_dir=output_dir, name=export_name, payload=payload, write_latest=write_latest)
    logger.info("%s export success: count=%s path=%s latest=%s", export_name, len(fields), paths.timestamped, paths.latest)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


if __name__ == "__main__":
    main()
