import pandas as pd
import numpy as np
import yfinance as yf
import requests
import csv
import sys
import time

from volpredict.paths import RAW_DIR, PROCESSED_DIR, MODELS_DIR, CONFIG_DIR

def ing():
	'''
	Install ticker data from start to end.
	"constituents.csv" holds S&P 500 from ~August 2025
	Replace for alternative testing
	'''	
	start = sys.argv[1]
	end = sys.argv[2]
	
	tickers = pd.read_csv(CONFIG_DIR / "constituents.csv")["Symbol"].to_numpy()	

	download_stockprices(tickers, "1d", start, end)


	return 1


def ticker_map():
	with open(CONFIG_DIR / f"ticker_wiki_map.csv", newline="", encoding="utf-8") as f:
		r = csv.reader(f)
		next(r, None)
		data = dict(r)
	return data


def download_pageviews(ticker, project, access, agent, article, granularity, start, end):
	'''
	Used for an earlier iteration, unused/deemed useless by random forest
	'''
	print(f"Downloading Wikipedia pageviews for {ticker} ({start} -> {end})")
	url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}")

	headers = {"User-Agent": "vol-ingest/1.0 (contact: jonathanjordanwork@gmail.com)"}

	resp = requests.get(url, headers=headers)
	resp.raise_for_status()
	data = resp.json()

	items = data.get("items", [])
	dates = [modify_date(it["timestamp"]) for it in items]
	views = [it["views"] for it in items]

	data = {"date": dates, 
			"views": views
	}

	df = pd.DataFrame(data)
	
	df.to_csv(RAW_DIR / f"{ticker}_views.csv", index=False)

def modify_date(date):
	return date[:4] + '-' + date[4:6] + '-' + date[6:8]

def download_stockprices(tickers, interval, start, end):
	for i in range(len(tickers)):
		df = yf.download(tickers[i],interval=interval, start=start, end=end)
		df.to_csv(RAW_DIR / f"{tickers[i]}_prices.csv")
		time.sleep(0.2)

def get_stockdata(ticker):
	df = pd.read_csv(RAW_DIR / f"{ticker}_prices.csv", index_col=0)

	# force index to datetime, non-dates become NaT
	df.index = pd.to_datetime(df.index, format="%Y-%m-%d", errors="coerce")
	df = df[df.index.notna()]
	df.index.name = 'Date'


	for col in ["Open", "High", "Low", "Close", "Volume"]:
		df[col] = pd.to_numeric(df[col], errors="coerce")

	return df

if __name__ == "__main__":
	ing()
