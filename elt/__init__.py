"""
FAME Data Space - ELT Package
==============================
Extract, Load, Transform pipeline using modern EtLT pattern.

Pattern: Extract → Light Transform → Load → Transform (in DW)
"""

from .extract import FAMEExtractor
from .load import FAMELoader
from .transform import FAMETransformer
from .warehouse import FAMEWarehouse
from .main_pipeline import FAMEPipeline

__all__ = [
    'FAMEExtractor',
    'FAMELoader',
    'FAMETransformer',
    'FAMEWarehouse',
    'FAMEPipeline'
]
