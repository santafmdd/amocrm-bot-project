import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.writers.google_sheets_layout_ui_writer import GoogleSheetsUILayoutWriter
from src.writers.models import CompiledProfileAnalyticsResult


def _compiled(*values: str) -> CompiledProfileAnalyticsResult:
    return CompiledProfileAnalyticsResult(
        report_id='analytics_tag_single_example',
        display_name='Analytics tag profile',
        generated_at=datetime(2026, 1, 1, 12, 0, 0),
        source_kind='tag',
        filter_values=list(values),
        tabs=['all', 'active', 'closed'],
        top_cards_by_tab={},
        stages_by_tab={},
        totals_by_tab={'all': 0, 'active': 0, 'closed': 0},
    )


def test_resolve_block_aliases_prefers_compiled_filter_values_when_generic_fallback_disabled() -> None:
    writer = GoogleSheetsUILayoutWriter(project_root=Path('.'))
    compiled = _compiled('conf_novosib_mechanical_engineering_2026 машэкспо')
    layout = {
        'tag_block_aliases': ['уралстрой', 'машэкспо', 'инглегмаш'],
        'allow_generic_tag_alias_fallback': False,
        'summary_block_aliases': ['все 3 выставки'],
    }

    resolved = writer._resolve_block_aliases(compiled, layout)

    assert resolved['tag_block'] == ['conf_novosib_mechanical_engineering_2026 машэкспо']
    assert resolved['summary_block'] == ['все 3 выставки']


def test_resolve_block_aliases_allows_generic_fallback_when_enabled() -> None:
    writer = GoogleSheetsUILayoutWriter(project_root=Path('.'))
    compiled = _compiled('conf_novosib_mechanical_engineering_2026 машэкспо')
    layout = {
        'tag_block_aliases': ['уралстрой', 'машэкспо'],
        'allow_generic_tag_alias_fallback': True,
    }

    resolved = writer._resolve_block_aliases(compiled, layout)

    assert resolved['tag_block'] == [
        'conf_novosib_mechanical_engineering_2026 машэкспо',
        'уралстрой',
        'машэкспо',
    ]


def test_resolve_block_aliases_uses_generic_when_compiled_filter_values_absent() -> None:
    writer = GoogleSheetsUILayoutWriter(project_root=Path('.'))
    compiled = _compiled()
    layout = {
        'tag_block_aliases': ['уралстрой', 'машэкспо'],
        'allow_generic_tag_alias_fallback': False,
    }

    resolved = writer._resolve_block_aliases(compiled, layout)

    assert resolved['tag_block'] == ['уралстрой', 'машэкспо']
