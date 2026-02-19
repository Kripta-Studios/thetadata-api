# ThetaData API v3 Wrapper

A modular, production-grade Python API for interacting with the **ThetaData Terminal** (Local v3).

## Features
- **ThetaClient**: Async HTTP client with automatic zero-repair for OHLC data.
- **Bulk Engine**: Multi-processed historical downloads for options and greeks.
- **Realtime Feed**: Live polling daemon for Machine Learning model consumption.
- **Calendar Utils**: Automatic holiday detection and VIX expiration logic.

## Installation
From the root directory, run:
```bash
pip install -e .