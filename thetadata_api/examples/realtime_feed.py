"""
Example 3: Real-time Feed for Machine Learning
----------------------------------------------
This script demonstrates how to start the RealtimeFeed daemon and 
consume its data to feed a Machine Learning model (MLP).
"""

import asyncio
import sys
import pandas as pd
from pathlib import Path

# Add the parent directory to the path so Python can find 'thetadata_api'
sys.path.append(str(Path(__file__).resolve().parents[1]))
from thetadata_api import RealtimeFeed

async def mock_mlp_model_consumer(feed: RealtimeFeed):
    """
    A mock consumer representing your MLP model logic.
    It periodically polls the feed for the latest data.
    """
    print("MLP Model Consumer started. Waiting for data...")
    
    # Wait a few seconds for the first poll cycle to finish
    await asyncio.sleep(10) 
    
    while True:
        # 1. Get the most recent state of all tracked series
        snapshot = feed.get_latest_snapshot()
        
        if not snapshot:
            print("No data in snapshot yet...")
        else:
            print(f"\n--- Model Input Snapshot at {pd.Timestamp.now()} ---")
            for key, df in snapshot.items():
                last_price = df['close'].iloc[-1] if not df.empty else "N/A"
                print(f"Key: {key:<20} | Rows: {len(df):>4} | Latest Price: {last_price}")
            
            # 2. Here you would convert the DataFrames to Tensors for your MLP
            # example: input_tensor = torch.tensor(df.values)
        
        # Poll the memory every 30 seconds for new model inputs
        await asyncio.sleep(30)

async def main():
    # Define the symbols you want to track in real-time
    symbols_to_track = ["SPX", "SPY", "QQQ", "VIX"]
    
    # Initialize the RealtimeFeed API
    feed = RealtimeFeed(
        symbols=symbols_to_track,
        poll_interval=60,         # Fetch new candles every 60 seconds
        output_dir="./rt_data"    # Live data also gets backed up here
    )

    print("="*60)
    print(" STARTING REAL-TIME MLP FEED DEMONSTRATOR ")
    print("="*60)

    # Run both the Feed (Producer) and the Model Consumer (Consumer) concurrently
    try:
        await asyncio.gather(
            feed.run_forever(),
            mock_mlp_model_consumer(feed)
        )
    except KeyboardInterrupt:
        print("\nStopping real-time feed...")

if __name__ == "__main__":
    # Ensure Theta Terminal is running on port 25503 before starting
    asyncio.run(main())