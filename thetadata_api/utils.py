import httpx
import time
import logging
import csv
import os
from typing import Dict, Any, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

def get_logger(name: str = "thetadata") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    return logger

class RetryAuditLog:
    def __init__(self, filename: str = "retry_audit.csv"):
        self.filename = filename
        self.headers = ["timestamp", "endpoint", "retry_count", "error_message"]
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
    
    def log_retry(self, endpoint: str, retry_count: int, error_message: str):
        with open(self.filename, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().isoformat(), endpoint, retry_count, error_message])

class RequestStats:
    def __init__(self, filename: str = "request_stats.csv"):
        self.filename = filename
        self.stats = defaultdict(list)
        self.headers = ["timestamp", "endpoint", "duration", "status_code"]
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
    
    def add_stat(self, endpoint: str, duration: float, status_code: int):
        with open(self.filename, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([datetime.now().isoformat(), endpoint, duration, status_code])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=6),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.RequestError)),
    reraise=True
)
async def timed_get(client: httpx.AsyncClient, url: str, params: Dict[str, Any], 
                   logger: logging.Logger, audit: RetryAuditLog, 
                   stats: RequestStats, endpoint: str) -> Tuple[Dict[str, Any], int]:
    start_time = time.time()
    try:
        response = await client.get(url, params=params, timeout=30.0)
        duration = time.time() - start_time
        stats.add_stat(endpoint, duration, response.status_code)
        
        if response.status_code != 200:
            raise httpx.HTTPStatusError(f"HTTP {response.status_code}: {response.text}", request=response.request, response=response)
        return response.json(), response.status_code
    except Exception as e:
        audit.log_retry(endpoint, 1, str(e))
        raise

async def fetch_with_interval_fallback(client: httpx.AsyncClient, url: str, params: Dict[str, Any],
                                      logger: logging.Logger, audit: RetryAuditLog, 
                                      stats: RequestStats, endpoint: str) -> Tuple[Dict[str, Any], str]:
    interval = params.get("interval", "1m")
    try:
        response, _ = await timed_get(client, url, params, logger, audit, stats, endpoint)
        return response, interval
    except Exception as e:
        logger.warning(f"Error fetching data from {endpoint}: {e}")
        raise

def parse_response(response: Dict[str, Any]) -> list:
    """Extracts the 'response' block from ThetaData v3."""
    if response.get("error"):
        raise Exception(f"API Error: {response.get('error')}")
    data = response.get("response", [])
    # DEBUG: print(f"Raw data from API: {data[:1]}") # Descomenta si el error persiste
    return data

def verify_data_integrity(data: pd.DataFrame) -> Dict[str, Any]:
    if data.empty:
        return {"valid": False, "message": "Empty data"}
    null_counts = data.isnull().sum()
    if null_counts.sum() > 0:
        return {"valid": False, "message": f"Null values found: {null_counts.to_dict()}"}
    return {"valid": True, "message": "Valid data"}

def fix_empty_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Fixes empty rows using modern Pandas syntax."""
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    data[numeric_columns] = data[numeric_columns].replace(0, np.nan)
    data = data.bfill().ffill() 
    return data