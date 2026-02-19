import asyncio
import logging
import os
import pandas as pd
from .client import ThetaClient
from .utils import verify_data_integrity, fix_empty_rows, get_logger

class Pipeline:
    """Orchestrator to download and clean data via the Client."""
    
    def __init__(self, client: ThetaClient):
        self.client = client
        self.logger = get_logger("Pipeline")

    def _save_parquet(self, df: pd.DataFrame, folder: str, filename: str):
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{filename}.parquet")
        df.to_parquet(path, index=False)
        self.logger.info(f"Saved: {path}")

    async def run_underlying_pipeline(self, symbols: list, date: str, output_dir: str = "data_underlying"):
        for symbol in symbols:
            try:
                self.logger.info(f"Starting underlying: {symbol}")
                underlying = await self.client.fetch_underlying_ohlc(symbol, date)
                
                integrity = verify_data_integrity(underlying.data)
                if not integrity["valid"]:
                    self.logger.warning(f"Fixing {symbol}: {integrity['message']}")
                    underlying.data = fix_empty_rows(underlying.data)

                self._save_parquet(underlying.data, output_dir, f"{symbol}_{date}")
            except Exception as e:
                self.logger.error(f"Error in underlying {symbol}: {e}")

    async def run_option_pipeline(self, symbols: list, date: str, output_dir: str = "data_options"):
        for symbol in symbols:
            try:
                self.logger.info(f"Starting options: {symbol}")
                
                # 1. Find closest expiration and central strike
                exps = await self.client.get_expirations(symbol, date)
                if not exps: continue
                target_exp = exps[0]
                
                strikes = await self.client.get_strikes(symbol, target_exp, date)
                if not strikes: continue
                target_strike = strikes[len(strikes)//2] # At-The-Money proxy strike
                
                # 2. Download Call OHLC
                opt_data = await self.client.fetch_option_data(
                    symbol, target_exp, target_strike, "C", date, "ohlc"
                )
                
                integrity = verify_data_integrity(opt_data.data)
                if not integrity["valid"]:
                    opt_data.data = fix_empty_rows(opt_data.data)

                filename = f"{symbol}_{target_exp}_{target_strike}C_{date}"
                self._save_parquet(opt_data.data, output_dir, filename)
                
            except Exception as e:
                self.logger.error(f"Error in option {symbol}: {e}")

    async def run_full_pipeline(self, target_date: str, underlying_symbols: list, option_symbols: list):
        """Executes the full pipeline for a specific date."""
        self.logger.info(f"Starting Pipeline for date {target_date}")
        await self.run_underlying_pipeline(underlying_symbols, target_date)
        await self.run_option_pipeline(option_symbols, target_date)
        self.logger.info("Pipeline completed.")