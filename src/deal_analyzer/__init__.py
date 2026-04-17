"""Deal analyzer MVP package."""

from .call_downloader import CallDownloader
from .call_evidence import build_call_summary, deduplicate_calls
from .config import DealAnalyzerConfig, load_deal_analyzer_config
from .enrichment import build_operator_outputs, enrich_rows
from .llm_backend import analyze_deal_with_ollama
from .models import DealAnalysis
from .roks_extractor import extract_roks_snapshot
from .rules import analyze_deal
from .snapshot_builder import build_deal_snapshot, build_period_snapshots

__all__ = [
    "CallDownloader",
    "build_call_summary",
    "deduplicate_calls",
    "DealAnalyzerConfig",
    "DealAnalysis",
    "analyze_deal",
    "analyze_deal_with_ollama",
    "build_operator_outputs",
    "build_deal_snapshot",
    "build_period_snapshots",
    "enrich_rows",
    "extract_roks_snapshot",
    "load_deal_analyzer_config",
]
