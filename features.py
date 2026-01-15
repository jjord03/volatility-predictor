import numpy as np
import calendar
import datetime
from ingest import *
from pathlib import Path

def main():
	df = get_stockdata("NVDA")
	const_matrix(df, "NVDA")



def const_matrix(stockdata, ticker):
    min_history=50
    vol_30 = yz_rolling(stockdata, N=30, min_history=min_history)
    vol_5 = yz_rolling(stockdata, min_history=min_history)
    volvol_wiki = wiki_feat(stockdata, ticker)
    volvol_volume = volume_z(stockdata["Volume"].to_numpy(), min_history=min_history)
    dto = np.array([days_to_opex(d) for d in stockdata.index])
    
    df_feat = pd.DataFrame(
        {
            "vol_30": vol_30,
            "vol_5": vol_5,
            "volvol_wiki": volvol_wiki,
            "volvol_volume": volvol_volume,
            "days_to_opex": dto
        },
        index=stockdata.index
    )
    
    df_feat = df_feat.iloc[30+min_history:]
    print(df_feat)

    return df_feat
    


def wiki_feat(stockdata, ticker):
    rolled_views = wiki_forward_fill(stockdata, ticker)

    wiki_vol = volume_z(rolled_views)

    return wiki_vol

def rolling_avg(arr, N):
	mean = 0

	for i in range(N - 1, len(arr)):
		mean += np.mean(arr[i-(N-1):i+1])

	mean /= (len(arr) - N + 1)

	return mean

def yz_rolling(df, N=5, min_history=20):
	open_ = df["Open"].to_numpy()
	high = df["High"].to_numpy()
	low = df["Low"].to_numpy()
	close = df["Close"].to_numpy()

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

def volume_z(volume, min_history=20, N=5):
    V = np.log1p(volume)

    window_mean = np.full(len(V), np.nan)
    for i in range(N-1, len(V)):
        window_mean[i] = np.mean(V[i - N + 1:i + 1])

    hist = window_mean[N-1:min_history]
    mu = np.mean(hist)
    sd = np.std(hist)

    scores = np.full(len(V), np.nan)

    for i in range(min_history + N - 1, len(V)):
        scores[i] = (window_mean[i] - mu) / sd

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

	return rolled_views

def days_to_opex(date):
	month = date.month
	year = date.year

	opex = find_third_friday(year, month)

	if opex <= date:
		month += 1
		if month > 12:
			month = 1
			year += 1

	opex = find_third_friday(year, month)

	return max((opex - date).days - 1, 0)

def find_third_friday(year, month):

	cal = calendar.monthcalendar(year, month)
	fridays = []

	for week in cal:
		if week[calendar.FRIDAY] != 0:
			fridays.append(week[calendar.FRIDAY])

	assert len(fridays) >= 3

	return pd.Timestamp(year=year, month=month, day=fridays[2])

if __name__ == "__main__":
    main()
