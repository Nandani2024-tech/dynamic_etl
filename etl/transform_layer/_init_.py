"""
Transform Layer Package
-----------------------

Modules included:
- transform_main: Orchestrates the full transform pipeline
- cleaning: Data cleaning functions
- validators: Structural & logical data validation
- normalization: Standardizes types & formatting
- enrichment: Adds derived/lookup/enhanced data
- converters: Final authoritative type conversions
- utils: Common shared helpers

Usage:
    from transform_layer import run_transform_pipeline
"""

from .transform_main import run_transform_pipeline
from . import cleaning
from . import validators
from . import normalization
from . import enrichment
from . import converters
from . import utils

__version__ = "1.0.0"

__all__ = [
    "run_transform_pipeline",
    "cleaning",
    "validators",
    "normalization",
    "enrichment",
    "converters",
    "utils",
    "__version__",
]
