# ThetaData API Client

This project provides a Python client for interacting with the Theta Terminal Local API (v3), offering functionality for fetching option data, underlying OHLC data, and managing real-time data feeds.

## Overview

The `ThetaClient` is designed to interact with the Theta Terminal Local API, enabling users to:

- Fetch option expirations and strikes
- Retrieve underlying OHLC data using Greeks (Spot Proxy)
- Download historical option data (OHLC and Greeks)
- Handle real-time data feeds
- Perform data correction and integrity checks

## Features

- Asynchronous HTTP client using `httpx`
- Automatic retry logic with exponential backoff
- Data integrity checks and zero-repair logic
- Real-time data polling for ML models
- Batch processing of historical option data
- Calendar-based holiday and trading day handling

## Installation

To install the required dependencies, run:

```bash
pip install httpx pandas numpy tenacity pyarrow
```

## Usage

### 1. Basic Client Usage

```python
from client import ThetaClient

client = ThetaClient(base_url="http://127.0.0.1:25503/v3")
```

### 2. Fetching Expirations

```python
exps = await client.get_expirations("SPX", "2024-04-15")
```

### 3. Fetching Strikes

```python
strikes = await client.get_strikes("SPX", "20240426", "2024-04-15")
```

### 4. Deriving Underlying OHLC

```python
underlying = await client.fetch_underlying_ohlc("SPX", "2024-04-15")
```

### 5. Real-time Data Feed

```python
from realtime import RealtimeFeed

feed = RealtimeFeed(["SPX", "VIX"], poll_interval=60)
await feed.run_forever()
```

### 6. Bulk Historical Data Download

```python
from bulk import download_historical_options

download_historical_options(
    symbols=["SPX", "VIX"],
    start_date="2024-01-01",
    end_date="2024-03-31",
    output_path="./data_options"
)
```

### 7. Pipeline Execution

```python
from pipeline import Pipeline

pipeline = Pipeline(client)
await pipeline.run_full_pipeline("2024-04-15", ["SPX"], ["SPX"])
```

## API Endpoints

- `/option/list/expirations`
- `/option/list/strikes`
- `/option/history/greeks/first_order`
- `/index/history/ohlc`
- `/stock/history/ohlc`
- `/calendar/year_holidays`

## Data Models

### UnderlyingData

Represents derived underlying data using Greeks (Spot Proxy).

### OptionData

Represents historical option data.

## Calendar Utilities

### `last_trading_day_of_week`

Finds the last valid trading day of the current week.

### `wednesday_of_week`

Returns the Wednesday of the current week.

### `get_next_valid_vix_expiration`

Finds the next valid VIX expiration starting from the current date.

### `select_target_expirations`

Selects target expirations (0DTE + Weekly) based on symbol logic.

## Error Handling

The client includes automatic retry logic with exponential backoff for transient network issues. Errors are logged and audited using `RetryAuditLog` and `RequestStats`.

## Data Correction

The `Corrector` module provides functions for:

- Zero-repair of OHLC data
- Gap-filling for missing values
- Batch processing of existing Parquet files

## Real-time Feed

The `RealtimeFeed` class provides a daemon that polls real-time data at regular intervals and maintains session data for ML model consumption.

## Bulk Data Processing

The `bulk.py` module allows for downloading historical option data in parallel across multiple processes, automatically handling weekends and holidays.

## Logging and Statistics

The system uses structured logging with `get_logger` and maintains statistics in CSV files for monitoring and debugging purposes.

## Dependencies

- `httpx` for asynchronous HTTP requests
- `pandas` for data manipulation
- `numpy` for numerical operations
- `tenacity` for retry logic
- `pyarrow` for Parquet file handling

## License

This project is licensed under the MIT License.