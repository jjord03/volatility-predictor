import pandas as pd
import numpy as np
import yfinance as yf
import requests



def main():
	# just some code im using to test stuff
	tickers = get_tickers()
	start = "20241212"
	end = "20260102"
	project = "en.wikipedia.org"
	agent = "user"
	article = "Nvidia"
	granularity = "daily"
	access = "all-access"
	#df = get_stockdata('NVDA')
	#print(df)
	download_pageviews("NVDA", project, access, agent, article, granularity, start, end)
	#download_stockprices(tickers, "1d", start, end)


def get_tickers(num:int = 50, seed: int=1):
	'''
	Randomly select num tickers from the S&P 500
	'''
	df = pd.read_csv('data/processed/SP500_01_03_23.csv')
	
	column = df['Symbol']

	tickers_num = (column.dropna().drop_duplicates().sample(n=num, random_state=seed)).tolist()

	return tickers_num

def download_pageviews(ticker, project, access, agent, article, granularity, start, end):

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
	
	df.to_csv(f"data/raw/{ticker}_views.csv", index=False)

def modify_date(date):
	return date[:4] + '-' + date[4:6] + '-' + date[6:8]

def download_stockprices(tickers, interval, start, end):
	for i in range(len(tickers)):
		df = yf.download(tickers[i],interval=interval, start=start, end=end)
		print(df)
		df.to_csv(f"data/raw/{tickers[i]}_prices.csv")

def get_stockdata(ticker):
	df = pd.read_csv(f"data/raw/{ticker}_prices.csv", index_col=0)

	# force index to datetime, non-dates become NaT
	df.index = pd.to_datetime(df.index, format="%Y-%m-%d", errors="coerce")
	df = df[df.index.notna()]
	df.index.name = 'Date'


	for col in ["Open", "High", "Low", "Close", "Volume"]:
		df[col] = pd.to_numeric(df[col], errors="coerce")

	return df

if __name__ == "__main__":
	main()