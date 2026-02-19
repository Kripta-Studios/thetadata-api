"""
ThetaData v3 Local API Wrapper
"""

from .client import ThetaClient
from .models import UnderlyingData, OptionData
from .pipeline import Pipeline
from .bulk import download_historical_options
from .realtime import RealtimeFeed
from .corrector import fix_dataframe, fix_ohlc_files

__version__ = "0.1.0"
__author__ = "Software Engineer"

__all__ = [
    "ThetaClient",
    "UnderlyingData",
    "OptionData",
    "Pipeline",
    "download_historical_options",
    "RealtimeFeed",
    "fix_dataframe",
    "fix_ohlc_files"
]