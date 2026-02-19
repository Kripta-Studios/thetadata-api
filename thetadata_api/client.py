import httpx
import pandas as pd
from typing import Dict, Any, List
from .models import OptionData, UnderlyingData
from .utils import timed_get, fetch_with_interval_fallback, parse_response, RequestStats, RetryAuditLog, get_logger
from .corrector import fix_dataframe

class ThetaClient:
    """HTTP Client to interact with Theta Terminal Local (v3)."""
    
    def __init__(self, base_url: str = "http://127.0.0.1:25503/v3"):
        self.base_url = base_url
        self.session = httpx.AsyncClient()
        self.logger = get_logger("ThetaClient")
        self.stats = RequestStats()
        self.audit = RetryAuditLog()
        
    def _format_date(self, date_str: str) -> str:
        """Converts YYYY-MM-DD to YYYYMMDD."""
        return date_str.replace("-", "")

    async def get_expirations(self, symbol: str, date: str) -> List[str]:
        """Fetches and parses valid expirations."""
        date_fmt = self._format_date(date)
        url = f"{self.base_url}/option/list/expirations"
        resp, _ = await fetch_with_interval_fallback(
            self.session, url, {"symbol": symbol, "date": date_fmt, "format": "json"}, 
            self.logger, self.audit, self.stats, "expirations"
        )
        data = parse_response(resp)
        
        processed_exps = []
        for item in data:
            val = item.get("expiration") if isinstance(item, dict) else str(item)
            if val:
                processed_exps.append(val.replace("-", ""))
        return sorted([exp for exp in processed_exps if exp >= date_fmt])

    async def get_strikes(self, symbol: str, expiration: str, date: str) -> List[float]:
        """Fetches strikes handling both list and dictionary response formats."""
        url = f"{self.base_url}/option/list/strikes"
        resp, _ = await fetch_with_interval_fallback(
            self.session, url, {"symbol": symbol, "expiration": expiration, "date": self._format_date(date), "format": "json"}, 
            self.logger, self.audit, self.stats, "strikes"
        )
        data = parse_response(resp)
        
        processed_strikes = []
        for item in data:
            val = item.get("strike") or item.get("value") if isinstance(item, dict) else item
            if val is not None:
                processed_strikes.append(float(val))
        return sorted(processed_strikes)

    async def fetch_underlying_ohlc(self, symbol: str, date: str, interval: str = "1m") -> UnderlyingData:
        """Derives underlying OHLC using Greeks (Spot Proxy) with improved error handling."""
        self.logger.info(f"Deriving underlying for {symbol} on {date}...")
        date_fmt = self._format_date(date)
        
        # 1. Get Expirations
        exps = await self.get_expirations(symbol, date)
        if not exps: 
            raise Exception(f"No expirations available for {symbol} on {date}")
        
        # Try the first 2 expirations to find data
        for target_exp in exps[:2]:
            # 2. Get Strikes
            strikes = await self.get_strikes(symbol, target_exp, date)
            if not strikes: 
                continue
                
            target_strike = strikes[len(strikes)//2] # Use ATM strike
            url = f"{self.base_url}/option/history/greeks/first_order"
            
            # 3. Try both Call and Put to increase chances of finding a quote
            for right in ["C", "P"]:
                params = {
                    "symbol": symbol, 
                    "expiration": target_exp, 
                    "strike": str(target_strike),
                    "right": right, 
                    "date": date_fmt, 
                    "interval": "1s", 
                    "format": "json"
                }
                
                resp, _ = await fetch_with_interval_fallback(
                    self.session, url, params, self.logger, self.audit, self.stats, "underlying_proxy"
                )
                
                raw_items = parse_response(resp)
                if not raw_items:
                    continue

                # Flatten v3 nested data if necessary
                rows = []
                for item in raw_items:
                    if isinstance(item, dict) and "data" in item:
                        rows.extend(item["data"])
                    else:
                        rows.append(item)
                
                df = pd.DataFrame(rows)
                if df.empty or 'underlying_price' not in df.columns:
                    continue

                # 4. Processing and Aggregation
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.dropna(subset=['underlying_price']).sort_values('timestamp')
                
                # Group by minute to create OHLC
                df['_minute'] = df['timestamp'].dt.floor('min')
                ohlc = df.groupby('_minute')['underlying_price'].agg(
                    open='first', high='max', low='min', close='last', volume='count'
                ).reset_index().rename(columns={'_minute': 'timestamp'})
                
                # Apply automatic zero-repair
                self.logger.info(f"Successfully derived {symbol} from {target_exp} {right}")
                ohlc = fix_dataframe(ohlc)
                
                return UnderlyingData(symbol=symbol, data=ohlc, date=date, interval=interval)

        raise Exception(f"Could not derive underlying price for {symbol} after trying multiple expirations.")
        
    async def close(self):
        await self.session.aclose()