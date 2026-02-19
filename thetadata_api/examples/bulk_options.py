"""
Example 1: Bulk Historical Options Download
-------------------------------------------
This script demonstrates how to use the thetadata_api to download 
massive amounts of historical option data (0DTE and Weeklies).

It replicates the two-phase approach:
  - Phase 1: Process the heaviest symbol (SPXW).
  - Phase 2: Process the remaining symbols in parallel.
"""

import multiprocessing
import sys
from pathlib import Path

# Add the parent directory to the path so Python can find 'thetadata_api'
sys.path.append(str(Path(__file__).resolve().parents[1]))
from thetadata_api import download_historical_options

def main():
    start_date = "2024-01-01"
    end_date = "2026-02-28"
    output_dir = "./data_options"

    print("="*60)
    print(" PHASE 1: DOWNLOADING SPXW OPTIONS ")
    print("="*60)
    # The API will automatically handle the parallel execution and holiday checks
    download_historical_options(
        symbols=["SPXW"],
        start_date=start_date,
        end_date=end_date,
        output_path=output_dir
    )

    print("\n" + "="*60)
    print(" PHASE 2: DOWNLOADING REMAINING SYMBOLS ")
    print("="*60)
    # Spawns up to 4 parallel processes, one for each symbol
    download_historical_options(
        symbols=["SPX", "SPY", "QQQ", "VIX"],
        start_date=start_date,
        end_date=end_date,
        output_path=output_dir
    )

    print("\nAll bulk option downloads completed successfully.")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()