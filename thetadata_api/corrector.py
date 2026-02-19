import pandas as pd
from pathlib import Path
import numpy as np
from .utils import get_logger

logger = get_logger("Corrector")

def fix_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies zero-repair and gap-filling logic directly to a DataFrame in memory.
    """
    if df.empty:
        return df

    df_clean = df.copy()
    cols = [c for c in ['open', 'high', 'low', 'close', 'underlying_price'] if c in df_clean.columns]
    
    if not cols: 
        return df_clean

    # 1. Replace 0 with NaN
    df_clean[cols] = df_clean[cols].replace(0, np.nan)

    # 2. Internal row repair (partial NAs in a single minute)
    for idx, row in df_clean.iterrows():
        if row[cols].isna().any() and not row[cols].isna().all():
            # Find the best valid value to fill the holes in this specific minute
            valid_val = row['close'] if pd.notna(row.get('close')) else row.combine_first(pd.Series(row[cols])).dropna().iloc[0]
            df_clean.loc[idx, cols] = row[cols].fillna(valid_val)

    # 3. Gap repair (Fully NaN rows -> backward then forward fill)
    null_mask = df_clean[cols].isna().all(axis=1)
    if null_mask.any():
        df_clean[cols] = df_clean[cols].bfill().ffill()

    return df_clean


def fix_ohlc_files(data_dir: str, symbols: list):
    """
    Legacy/Batch API to iterate through directories and repair zero-gaps in existing Parquet files.
    """
    base_dir = Path(data_dir)
    logger.info("STARTING ZERO-REPAIR BATCH PROCESS")
    total_files_fixed = 0

    for symbol in symbols:
        logger.info(f"Checking {symbol}...")
        symbol_dir = base_dir / symbol
        if not symbol_dir.exists(): 
            continue
        
        for file_path in symbol_dir.rglob("*.parquet"):
            try:
                df_original = pd.read_parquet(file_path)
                df_fixed = fix_dataframe(df_original)
                
                # Only save if the dataframe was actually modified
                if not df_original.equals(df_fixed):
                    df_fixed.to_parquet(file_path, compression="snappy", index=False)
                    logger.info(f"[FIXED] {symbol} | Sanitized file: {file_path.name}")
                    total_files_fixed += 1
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")

    logger.info(f"PROCESS FINISHED. Files fixed: {total_files_fixed}")