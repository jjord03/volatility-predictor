## Structure

volatility-predictor/
|-- src/
|   |-- volpredict/
|   |   |-- __init__.py
|   |   |-- paths.py         # Centralized project paths (data/, config/, etc.)
|   |   |-- ingest.py        # Pulls price history for configured tickers -> data/raw/
|   |   |-- features.py      # Feature engineering + target (forward log realized vol)
|   |   |-- train.py         # Model training utilities (Ridge + time-series CV)
|   |   |-- predict.py       # Backtest / evaluation + HAR-style baseline + plots
|
|-- config/
|   |-- constituents.csv     # Ticker universe (expects a "Symbol" column)
|
|-- data/
|   |-- raw/                 # Downloaded prices land here (e.g. AAPL_prices.csv)
|
|-- README.m

## Install
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .

## Run

## 1.) download raw prices (write data/raw/<Ticker>_prices.csv)
Ex: python3 -m volpredict.ingest 2015-06-01 2026-02-01

## 2.) bulid features + train + evaluate + plots
Ex: python3 -m volpredict.predict
