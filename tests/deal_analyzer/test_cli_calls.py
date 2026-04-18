import sys
from unittest.mock import patch

from src.deal_analyzer.cli import _parse_args


def test_cli_parses_collect_calls_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "collect-calls",
        "--input",
        "workspace/amocrm_collector/deal_1_latest.json",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "collect-calls"


def test_cli_parses_transcribe_period_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "transcribe-period",
        "--input",
        "workspace/amocrm_collector/collect_period_latest.json",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "transcribe-period"


def test_cli_parses_build_call_snapshot_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "build-call-snapshot",
        "--input",
        "workspace/amocrm_collector/deal_1_latest.json",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "build-call-snapshot"


def test_cli_parses_janitor_report_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "janitor-report",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "janitor-report"


def test_cli_parses_janitor_clean_apply_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "janitor-clean",
        "--apply",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "janitor-clean"
    assert args.apply is True
