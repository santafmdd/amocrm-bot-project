from __future__ import annotations

from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.text_style_editor import daily_text_lint_failed, edit_rows, lint_daily_rows


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="deepseek-v3.1:671b-cloud",
        ollama_timeout_seconds=60,
        ollama_fallback_enabled=True,
        ollama_fallback_base_url="http://127.0.0.1:11434",
        ollama_fallback_model="deepseek-v3.1:671b-cloud",
        ollama_fallback_timeout_seconds=60,
    )


def _test_dir(name: str) -> Path:
    path = Path("workspace/tmp_tests/style_editor_tests").resolve() / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_daily_style_editor_performs_only_technical_cleanup() -> None:
    rows = [
        {
            "manager_name": "Илья Бочков",
            "control_day_date": "2026-04-24",
            "deal_ids": "123",
            "what_to_tell_employee": "  разобрать выявления ЛПР\n\nи закрепить шаг \x01 ",
        }
    ]
    result = edit_rows(
        mode="daily_control",
        rows=rows,
        run_id="style_daily_tech",
        project_root=_test_dir("daily_tech"),
        enable_llm_editor=False,
        cfg=None,
        llm_runtime=None,
        logger=None,
    )
    out = result["rows"][0]["what_to_tell_employee"]
    assert "\n" not in out
    assert "\x01" not in out
    assert "разобрать выявления ЛПР" in out  # no semantic rewrite in deterministic cleaner


def test_daily_lint_blocks_foreign_language_and_greetings() -> None:
    rows = [
        {
            "manager_name": "Илья Бочков",
            "deal_ids": "123",
            "what_to_tell_employee": "hello, clarifying decision-makers and gathering contacts",
        }
    ]
    lint = lint_daily_rows(rows)
    assert int(lint.get("foreign_greeting_count", 0)) > 0
    assert int(lint.get("foreign_language_count", 0)) > 0
    assert daily_text_lint_failed(lint) is True


def test_daily_lint_does_not_block_style_warnings_only() -> None:
    rows = [
        {
            "manager_name": "Рустам Хомидов",
            "deal_ids": "31134627",
            "what_to_fix": "разобрать выявления лпр",
        }
    ]
    lint = lint_daily_rows(rows)
    assert int(lint.get("bad_grammar_marker_count", 0)) > 0
    assert daily_text_lint_failed(lint) is False


def test_style_editor_preserves_protected_fields() -> None:
    rows = [
        {
            "period_start": "2026-04-20",
            "period_end": "2026-04-24",
            "control_day_date": "2026-04-24",
            "manager_name": "Илья Бочков",
            "deal_ids": "31134627;31937760",
            "deal_links": "https://example/a",
            "score_0_100": 44,
            "criticality": "medium",
            "main_pattern": "Текст\nс переносом",
            "expected_quant_impact": "+2-4 ЛПР в неделю",
        }
    ]
    result = edit_rows(
        mode="daily_control",
        rows=rows,
        run_id="style_daily_protected",
        project_root=_test_dir("daily_protected"),
        enable_llm_editor=False,
        cfg=None,
        llm_runtime=None,
        logger=None,
    )
    out = result["rows"][0]
    assert out["deal_ids"] == "31134627;31937760"
    assert out["deal_links"] == "https://example/a"
    assert out["control_day_date"] == "2026-04-24"
    assert out["score_0_100"] == 44
    assert out["expected_quant_impact"] == "+2-4 ЛПР в неделю"


def test_daily_llm_rewrite_rejected_when_protected_fields_changed(monkeypatch) -> None:
    def _fake_rewrite_rows_with_llm(*, base_url, model, timeout_seconds, mode, rows, fields):
        _ = base_url, model, timeout_seconds, mode, fields
        rewritten = []
        for item in rows:
            rewritten.append(
                    {
                        "row_index": item["row_index"],
                        "fields": {
                            "what_to_tell_employee": "你好",
                            "expected_quant_impact": "+9-9 ЛПР в неделю",
                        },
                    }
                )
        return rewritten, {"ok": True, "error": "", "model": model, "rows_used": len(rows), "repair_used": False}

    monkeypatch.setattr(
        "src.deal_analyzer.text_style_editor.rewrite_rows_with_llm",
        _fake_rewrite_rows_with_llm,
    )

    rows = [
        {
            "period_start": "2026-04-20",
            "period_end": "2026-04-24",
            "control_day_date": "2026-04-24",
            "manager_name": "Илья Бочков",
            "score_0_100": 44,
            "criticality": "medium",
            "what_to_tell_employee": "исходный текст",
            "expected_quant_impact": "+2-4 ЛПР в неделю",
        }
    ]

    runtime = {
        "selected": "main",
        "main": {"model": "deepseek-v3.1:671b-cloud", "base_url": "http://127.0.0.1:11434", "timeout_seconds": 30},
    }

    result = edit_rows(
        mode="daily_control",
        rows=rows,
        run_id="style_daily_reject",
        project_root=_test_dir("daily_reject"),
        enable_llm_editor=True,
        cfg=_cfg(),
        llm_runtime=runtime,
        logger=None,
    )

    out = result["rows"][0]
    assert out["what_to_tell_employee"] == "исходный текст"
    assert result["metrics"]["rejected_rewrites_count"] >= 1


def test_call_review_style_editor_preview_keeps_protected_fields() -> None:
    rows = [
        {
            "Deal ID": "32162059",
            "Сделка": "Сделка demo",
            "Менеджер": "Илья Бочков",
            "Дата кейса": "2026-04-24",
            "Прослушанные звонки": "2026-04-24 11:59 - 05:51",
            "Комментарий по этапу (лпр)": "  Текст\nс переносом  ",
        }
    ]
    result = edit_rows(
        mode="call_review",
        rows=rows,
        run_id="style_call_review",
        project_root=_test_dir("call_review"),
        enable_llm_editor=False,
        cfg=None,
        llm_runtime=None,
        logger=None,
    )
    out = result["rows"][0]
    assert out["Deal ID"] == "32162059"
    assert out["Комментарий по этапу (лпр)"] == "Текст с переносом"
    style_dir = Path(result["style_dir"])
    assert (style_dir / "style_editor_output.json").exists()
