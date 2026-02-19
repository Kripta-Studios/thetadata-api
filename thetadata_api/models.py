from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

@dataclass
class UnderlyingData:
    """Derived underlying data (Spot Proxy)."""
    symbol: str
    data: pd.DataFrame
    date: str
    interval: str
    
    def __post_init__(self):
        if self.data.empty:
            raise ValueError(f"Underlying data for {self.symbol} is empty.")

@dataclass
class OptionData:
    """Historical option data."""
    symbol: str
    expiration: str
    strike: float
    right: str
    data: pd.DataFrame
    date: str
    interval: str
    data_type: str
    
    def __post_init__(self):
        if self.data.empty:
            raise ValueError(f"Option data for {self.symbol} is empty.")