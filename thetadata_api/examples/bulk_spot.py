"""
Example 2: Historical Underlying Derivation (Spot Proxy)
--------------------------------------------------------
This script uses the ThetaClient to historically derive the 
underlying price of indices/stocks from their option Greeks.
It utilizes multiprocessing to fetch multiple symbols concurrently.
"""

import asyncio
import multiprocessing
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add the parent directory to the path so Python can find 'thetadata_api'
sys.path.append(str(Path(__file__).resolve().parents[1]))
from thetadata_api import ThetaClient

async def download_underlying_range(symbol: str, start_date: str, end_date: str, output_dir: str):
    """Async loop to fetch underlying data day by day for a single symbol."""
    client = ThetaClient()
    s_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    e_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    out_path = Path(output_dir)
    
    try:
        current_date = s_date
        while current_date <= e_date:
            # Skip standard weekends
            if current_date.weekday() < 5:
                date_str = current_date.strftime("%Y-%m-%d")
                date_compact = current_date.strftime("%Y%m%d")
                
                try:
                    # 1. API Magic: Fetches ATM strike, 1s Greeks, aggregates OHLC, AND applies zero-repair
                    underlying_data = await client.fetch_underlying_ohlc(symbol, date_str)
                    
                    # 2. Save to Parquet
                    save_dir = out_path / symbol / str(current_date.year) / f"{current_date.month:02d}"
                    save_dir.mkdir(parents=True, exist_ok=True)
                    file_name = save_dir / f"{symbol}_{date_compact}.parquet"
                    
                    underlying_data.data.to_parquet(file_name, index=False, compression="snappy")
                    print(f"[{symbol}] Successfully saved data for {date_str}")
                    
                except Exception as e:
                    # Usually means it was a market holiday or data was unavailable
                    print(f"[{symbol}] Skipped {date_str}: {e}")
            
            current_date += timedelta(days=1)
            
    finally:
        await client.close()

def _worker_bridge(args):
    """Bridge function to run asyncio loops inside multiprocessing workers."""
    symbol, start_date, end_date, output_dir = args
    asyncio.run(download_underlying_range(symbol, start_date, end_date, output_dir))

def main():
    symbols = ["SPXW", "SPY", "QQQ", "VIX"]
    start_date = "2024-01-01"
    end_date = "2026-02-28"
    output_dir = "./data_underlying_derived"
    
    print("="*60)
    print(" STARTING HISTORICAL SPOT PROXY DERIVATION ")
    print("="*60)
    
    # Prepare arguments for each worker
    tasks = [(sym, start_date, end_date, output_dir) for sym in symbols]
    
    # Spawn a process for each symbol to download concurrently
    with multiprocessing.Pool(processes=len(symbols)) as pool:
        pool.map(_worker_bridge, tasks)
        
    print("\nUnderlying derivation completed successfully.")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()