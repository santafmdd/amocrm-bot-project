"""Deal analyzer MVP package."""

from .config import DealAnalyzerConfig, load_deal_analyzer_config
from .models import DealAnalysis
from .rules import analyze_deal

__all__ = [
    "DealAnalyzerConfig",
    "DealAnalysis",
    "analyze_deal",
    "load_deal_analyzer_config",
]
