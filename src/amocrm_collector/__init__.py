"""amoCRM collector package (read-only)."""

from .config import AmoCollectorConfig, load_collector_config
from .normalizer import AmoDealNormalizer

__all__ = ["AmoCollectorConfig", "AmoDealNormalizer", "load_collector_config"]
