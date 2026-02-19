import asyncio
import multiprocessing
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta
from pathlib import Path

from .client import ThetaClient
from .utils import fetch_with_interval_fallback, parse_response, get_logger
from .calendar_utils import select_target_expirations

ENDPOINTS = {"ohlc": "/option/history/ohlc", "greeks": "/option/history/greeks/first_order"}

async def _worker_main(symbol: str, start_date: date, end_date: date, output_dir: Path):
    """Internal function that performs the workload per process."""
    client = ThetaClient()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        closed_dates = set()
        years_to_fetch = range(start_date.year, end_date.year + 1)
        
        for year in years_to_fetch:
            resp, _ = await fetch_with_interval_fallback(
                client.session, f"{client.base_url}/calendar/year_holidays",
                {"year": str(year), "format": "json"},
                client.logger, client.audit, client.stats, "calendar"
            )
            
            if resp and resp.status_code == 200:
                entries = parse_response(resp.json())
                for entry in entries:
                    if isinstance(entry, dict) and entry.get("type") == "full_close":
                        try:
                            closed_dates.add(date.fromisoformat(entry["date"]))
                        except Exception:
                            pass
                            
        client.logger.info(f"[{symbol}] Holidays loaded: {len(closed_dates)} closed days found.")

        d = start_date
        trading_days = []
        while d <= end_date:
            if d.weekday() < 5 and d not in closed_dates:
                trading_days.append(d)
            d += timedelta(days=1)

        for day in trading_days:
            exps_str = await client.get_expirations(symbol, day.strftime("%Y%m%d"))
            if not exps_str: continue
            
            avail_dates = [date(int(e[:4]), int(e[4:6]), int(e[6:8])) for e in exps_str]
            targets = select_target_expirations(symbol, day, avail_dates, closed_dates)
            
            for exp in targets:
                for dtype, endpoint in ENDPOINTS.items():
                    params = {
                        "symbol": symbol, "expiration": exp.strftime("%Y%m%d"),
                        "date": day.strftime("%Y%m%d"), "strike": "*", "right": "both", "format": "json"
                    }
                    
                    url = f"{client.base_url.replace('/v3','')}{endpoint}"
                    
                    resp, _ = await fetch_with_interval_fallback(
                        client.session, url, params, client.logger, client.audit, client.stats, f"bulk_{dtype}"
                    )
                    
                    if not resp or resp.status_code != 200: continue
                    
                    items = parse_response(resp.json())
                    rows = []
                    
                    for item in items:
                        if isinstance(item, dict) and "contract" in item and "data" in item:
                            contract = item["contract"]
                            for datarow in item.get("data", []):
                                rows.append({**contract, **datarow})

                    if rows:
                        df = pd.DataFrame(rows)
                        save_dir = output_dir / symbol / dtype / str(day.year) / f"{day.month:02d}"
                        save_dir.mkdir(parents=True, exist_ok=True)
                        fname = save_dir / f"{symbol}_{exp.strftime('%Y%m%d')}_{day.strftime('%Y%m%d')}_{dtype}.parquet"
                        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), fname, compression="snappy")
                        client.logger.info(f"Bulk Saved: {fname.name}")

    except Exception as e:
        client.logger.error(f"Error processing {symbol}: {e}")
    finally:
        await client.close()

def _run_worker(args):
    """Bridge necessary to isolate the asyncio Event Loop in multiprocessing."""
    symbol, start_date, end_date, output_dir = args
    asyncio.run(_worker_main(symbol, start_date, end_date, output_dir))

def download_historical_options(symbols: list, start_date: str, end_date: str, output_path: str = "./data_options"):
    """
    Public API: Downloads option history (OHLC and Greeks) for a list of symbols in a date range.
    Automatically dodges weekends and real market holidays.
    """
    s_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    e_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    out_dir = Path(output_path)
    
    logger = get_logger("BulkEngine")
    logger.info(f"Launching Bulk Engine for {symbols} from {s_date} to {e_date}...")
    
    tasks = [(sym, s_date, e_date, out_dir) for sym in symbols]
    num_processes = min(len(symbols), 4)
    
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(_run_worker, tasks)
        
    logger.info("Bulk Download Finished.")