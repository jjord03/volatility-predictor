# volatility-predictor

A small research project for downloading equity price history, engineering volatility features, training a simple model with time-series cross-validation, and running an evaluation/backtest (including a HAR-style baseline).

---

## Project layout

```text
volatility-predictor/
├─ src/
│  └─ volpredict/
│     ├─ __init__.py
│     ├─ paths.py        # Centralized project paths (data/, config/, etc.)
│     ├─ ingest.py       # Download price history for configured tickers -> data/raw/
│     ├─ features.py     # Feature engineering + target (forward log realized vol)
│     ├─ train.py        # Model training utilities (Ridge + time-series CV)
│     └─ predict.py      # Backtest/evaluation + HAR-style baseline + plots
├─ config/
│  └─ constituents.csv   # Ticker universe (expects a "Symbol" column)
├─ data/
│  └─ raw/               # Downloaded prices land here (e.g., AAPL_prices.csv)
├─ pyproject.toml
└─ README.md
```

---

## Install

Create a virtual environment and install the package in editable mode:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
```

---

## Configuration

Ticker universe is read from:

- `config/constituents.csv`

It must contain a `Symbol` column, for example:

```csv
Symbol
AAPL
MSFT
SPY
```

---

## Run

### 1) Download raw prices

This writes CSVs to `data/raw/` named like `AAPL_prices.csv`.

```bash
python3 -m volpredict.ingest 2015-06-01 2026-02-01
```

**Arguments:**
- `START_DATE` (YYYY-MM-DD)
- `END_DATE` (YYYY-MM-DD)

---

### 2) Build features, train, evaluate, and plot

Runs the pipeline (feature engineering → model training → evaluation/backtest → plots).

```bash
python3 -m volpredict.predict
```

---

## Notes

- If `data/raw/` does not exist, create it (or ensure the code creates it automatically).
- If downloads fail for a subset of tickers, verify symbols in `config/constituents.csv`.
