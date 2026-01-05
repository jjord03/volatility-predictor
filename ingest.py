import pandas as pd
import numpy as np
import yfinance as yf
import requests


def main():
	# just some code im using to test stuff
	tickers = get_tickers()
	start = "2023-01-09"
	end = "2026-01-2"
	project = "en.wikipedia.org"
	agent = "user"
	article = "Nvidia"
	granularity = "daily"
	access = "all-access"
	#get_pageviews(project, access, agent, article, granularity, start, end)
	download_stockprices(tickers, "1d", start, end)


def get_tickers(num:int = 50, seed: int=1):
	'''
	Randomly select num tickers from the S&P 500
	'''
	df = pd.read_csv('data/processed/SP500_01_03_23.csv')
	
	column = df['Symbol']

	tickers_num = (column.dropna().drop_duplicates().sample(n=num, random_state=seed)).tolist()

	print(len(tickers_num))

	return tickers_num

def get_pageviews(project, access, agent, article, granularity, start, end):

	url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}")

	headers = {"User-Agent": "BeginnerPageviewsScript"}

	resp = requests.get(url, headers=headers)
	resp.raise_for_status()
	data = resp.json()

	items = data.get("items", [])
	dates = [it["timestamp"] for it in items]
	views = [it["views"] for it in items]

	print(dates)
	print(views)
	return dates, views

def download_stockprices(tickers, interval, start, end):
	for i in range(len(tickers)):
		df = yf.download(tickers[i],interval=interval, start=start, end=end)
		df.to_csv(f"data/raw/{tickers[i]}_prices.csv")

def get_stockdata(ticker):
	df = pd.read_csv(f"data/raw/{ticker}_prices.csv")
	
	closing_data = df['Close']
	high_data = df['High']
	low_data = df['Low']
	open_data = df['Open']
	volume_data = df['Volume']

	# Isolate values
	close = closing_data.tolist()
	close = np.asarray(close[2:], dtype=float)

	high = high_data.tolist()
	high = np.asarray(high[2:], dtype=float)

	low = low_data.tolist()
	low = np.asarray(low[2:], dtype=float)

	open_ = open_data.tolist()
	open_ = np.asarray(open_[2:], dtype=float)

	volume = volume_data.tolist()
	volume = np.asarray(volume[2:], dtype=float)

	return close, high, low, open_, volume

if __name__ == "__main__":
	main()