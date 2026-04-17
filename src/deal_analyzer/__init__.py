"""Deal analyzer MVP package."""

from .config import DealAnalyzerConfig, load_deal_analyzer_config
from .llm_backend import analyze_deal_with_ollama
from .models import DealAnalysis
from .rules import analyze_deal

__all__ = [
    "DealAnalyzerConfig",
    "DealAnalysis",
    "analyze_deal",
    "analyze_deal_with_ollama",
    "load_deal_analyzer_config",
]
