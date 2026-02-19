import asyncio
import os
import pandas as pd
from datetime import date, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from .client import ThetaClient
from .utils import fetch_with_interval_fallback, parse_response, get_logger

class RealtimeFeed:
    """Daemon class to fetch real-time data and serve it to ML models."""
    
    def __init__(self, symbols: list, poll_interval: int = 60, output_dir: str = "./rt_data"):
        self.session_data = {}
        self.symbols = symbols
        self.client = ThetaClient()
        self.ET = ZoneInfo("America/New_York")
        self.poll_interval = poll_interval
        
        self.output_dir = Path(output_dir) / date.today().strftime("%Y%m%d")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.market_open = "09:30:00"
        self.last_candle_time = {}
        self.logger = get_logger("RealtimeFeed")

    def get_latest_snapshot(self) -> dict:
        """Public API to feed the MLP model."""
        return {k: v.copy() for k, v in self.session_data.items()}

    def _update_session(self, key: str, df: pd.DataFrame):
        if key in self.session_data:
            self.session_data[key] = pd.concat([self.session_data[key], df], ignore_index=True)
            self.session_data[key].drop_duplicates(subset=["timestamp", "strike", "right"], keep="last", inplace=True)
        else:
            self.session_data[key] = df.copy()

    async def poll_cycle(self):
        now_str = datetime.now(self.ET).strftime("%H:%M:%S")
        self.logger.info(f"Realtime Poll at {now_str} ET")
        
        for symbol in self.symbols:
            key = f"{symbol}_underlying"
            start_t = self.last_candle_time.get(key, self.market_open)
            
            endpoint = "/index/history/ohlc" if symbol in ["SPX", "VIX"] else "/stock/history/ohlc"
            resp, _ = await fetch_with_interval_fallback(
                self.client.session, f"{self.client.base_url.replace('/v3','')}{endpoint}",
                {"symbol": symbol, "start_date": date.today().strftime("%Y%m%d"), "end_date": date.today().strftime("%Y%m%d"), 
                 "start_time": start_t, "end_time": now_str, "format": "json"},
                self.client.logger, self.client.audit, self.client.stats, f"rt_{symbol}"
            )
            
            if resp and resp.status_code == 200:
                raw = parse_response(resp.json())
                if raw:
                    df = pd.DataFrame(raw)
                    self._update_session(key, df)
                    self.last_candle_time[key] = df.iloc[-1]["timestamp"][11:19]
                    self.logger.info(f"{symbol} updated: {len(df)} new candles.")

    async def run_forever(self):
        """Starts the infinite polling loop."""
        self.logger.info(f"Starting RealtimeFeed for {self.symbols}...")
        try:
            while True:
                await self.poll_cycle()
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            self.logger.info("RealtimeFeed stopped.")
        finally:
            await self.client.close()