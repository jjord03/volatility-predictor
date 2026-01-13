import numpy as np
from ingest import *
from pathlib import Path

def main():
	#min_history = 50
	#close, high, low, open_, volume = get_stockdata("TJX")
	#five_day = yz_rolling(min_history, open_, high, low, close)
	#twenty_day = yz_rolling(min_history, open_, high, low, close, N=20)
	#volume = volume_z(volume, min_history)
	df = get_stockdata("NVDA")
	wiki_forward_fill(df, "NVDA")

def feature_matrix():
	return

def rolling_avg(arr, N):
	mean = 0

	for i in range(N - 1, len(arr)):
		mean += np.mean(arr[i-(N-1):i+1])

	mean /= (len(arr) - N + 1)

	return mean


def yz_rolling(min_history, open_, high, low, close, N=5):
	assert N > 1
	n = len(open_)

	if not (len(high) == len(low) == len(close) == n):
		raise ValueError("All array must have the same length")

	o = np.full(len(close), np.nan)
	o[1:] = np.log(open_[1:] / close[:-1])

	c = np.log(close / open_)

	u = np.log(high / open_)

	d = np.log(low / open_)

	rs = u * (u - c) + d * (d - c)

	yz = np.full(n, np.nan)

	k = 0.34 / (1.34 + (N + 1) / (N - 1))

	for t in range(min_history + N - 1, n):

		# Window from s to t inclusive
		s = t - N + 1

		var_o = np.var(o[s:t+1], ddof=1)
		var_c = np.var(c[s:t+1], ddof=1)
		var_rs = np.mean(rs[s:t+1])

		yz[t] = np.sqrt(var_o + k * var_c + (1 - k) * var_rs)

	return yz

def volume_z(volume, min_history, N=5):
	log_vol = np.log1p(volume)
	# Initialize the rolling mean with min_history # days behind
	rol_vol = np.full(len(log_vol), np.nan)	

	for i in range(N - 1, len(rol_vol)):
		rol_vol[i] = np.mean(log_vol[i-(N-1):i+1])

	scores = np.full(len(log_vol), np.nan)

	for i in range(min_history + N - 1, len(log_vol)):
		# Volatility in my 5 day window
		vol_window = rol_vol[i]
		std = np.std(rol_vol[N - 1:i])
		rl_mean = np.mean(rol_vol[N - 1:i])

		scores[i] = (vol_window - rl_mean) / std

	return scores

def wiki_forward_fill(stockdata, ticker):
	if not Path(f"data/raw/{ticker}_views.csv").exists():
		print(f"{ticker} Wikipedia data does not exist")

	# read wiki
	wiki_data = pd.read_csv(f"data/raw/{ticker}_views.csv")
	wiki_data["date"] = pd.to_datetime(wiki_data["date"], format="%Y-%m-%d")
	wiki_data = wiki_data.set_index("date")
	wiki_data.index.name = "Date"

	trading_days = stockdata.index

	# map each wiki day to the next trading day >= wiki day
	pos = trading_days.searchsorted(wiki_data.index, side="left")
	valid = pos < len(trading_days)

	# keep wiki rows but make out of range ones NaN
	wiki_data.loc[~valid, "views"] = np.nan

	# holds the date that each row points to for wiki data
	next_trade = trading_days[pos[valid]]

	# aggregate into buckets. Sum views + number of days rolled into that trading day
	agg = wiki_data.loc[valid, "views"].groupby(next_trade).agg(["sum", "count"])

	# average out the data
	rolled_views = (agg["sum"] / agg["count"]).reindex(trading_days)

	print(rolled_views)

	return






	


if __name__ == "__main__":
	main()