import sys
from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.cli import _parse_args


def test_cli_parses_enrich_deal_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "enrich-deal",
        "--input",
        "workspace/amocrm_collector/deal_1_latest.json",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "enrich-deal"
    assert args.input.endswith("deal_1_latest.json")


def test_cli_parses_enrich_period_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "enrich-period",
        "--input",
        "workspace/amocrm_collector/collect_period_latest.json",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "enrich-period"


def test_cli_parses_roks_snapshot_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "roks-snapshot",
        "--manager",
        "Илья",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "roks-snapshot"
    assert args.manager == "Илья"
    assert args.team is False


def test_cli_parses_roks_snapshot_team_mode():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "roks-snapshot",
        "--team",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "roks-snapshot"
    assert args.team is True
