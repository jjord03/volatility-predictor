import numpy as np
import pandas as pd
import calendar
import datetime
import pandas_market_calendars as mcal

from volpredict.ingest import get_stockdata

nyse = mcal.get_calendar("NYSE")

def const_matrix(ticker, horizon=5, min_history=90):
	stockdata = get_stockdata(ticker)
	# Realized volatility
	RV_5 = realized_vol(stockdata, N=5)
	RV_10 = realized_vol(stockdata, N=10)
	RV_22 = realized_vol(stockdata, N=22)
	RV_60 = realized_vol(stockdata, N=60)

	RV_5  = pd.Series(RV_5,  index=stockdata.index)
	RV_10 = pd.Series(RV_10, index=stockdata.index)
	RV_22 = pd.Series(RV_22, index=stockdata.index)
	RV_60 = pd.Series(RV_60, index=stockdata.index)	
	# Log Realized Vol
	log_RV_5 = np.log(RV_5.replace(0, np.nan))
	log_RV_10 = np.log(RV_10.replace(0, np.nan))
	log_RV_22 = np.log(RV_22.replace(0, np.nan))
	log_RV_60 = np.log(RV_60.replace(0, np.nan))


	# Realized volatility ratios
	RV_5_22 = RV_5 / RV_22
	RV_5_60 = RV_5 / RV_60
	RV_10_60 = RV_10 / RV_60	

	# Change in volatility
	dlog_RV_5 = np.log(RV_5.replace(0, np.nan)).diff()
	dlog_RV_10 = np.log(RV_10.replace(0, np.nan)).diff()
	dlog_RV_22 = np.log(RV_22.replace(0, np.nan)).diff()
	# Vol of Vol
	vov_RV_5 = dlog_RV_5.rolling(5).std()
	vov_RV_10 = dlog_RV_10.rolling(10).std()

	# Log returns
	r1 = np.log(stockdata["Close"]).diff()
	r5 = r1.rolling(5).sum()
	r22 = r1.rolling(22).sum()
	r60 = r1.rolling(60).sum()

	# Volume feats
	v_5 = volume_z(stockdata, N=5, min_history=min_history)
	v_22 = volume_z(stockdata, N=22, min_history=min_history)
	v_60 = volume_z(stockdata, N=60, min_history=min_history)
	
	# Range-based
	hl_range = np.log(stockdata["High"] / stockdata["Low"])
	hl_mean_5 = hl_range.rolling(5).mean()
	hl_mean_22 = hl_range.rolling(22).mean()

	dte_opex = np.array([days_to_opex(d) for d in stockdata.index])
	vol_h = pd.Series(realized_vol(stockdata, N=horizon), index=stockdata.index)
	log_RV_h = np.log(vol_h.replace(0, np.nan))
	y = log_RV_h.shift(-horizon)

	df_feat = pd.DataFrame(
	{
		"y": y,
		# log vol levels
		"log_RV_5": log_RV_5,
		"log_RV_10": log_RV_10,
		"log_RV_22": log_RV_22,
		#"log_RV_60": log_RV_60,		

		# term struct
		"RV_5_22": RV_5_22,
		#"RV_5_60": RV_5_60,
		"RV_10_60": RV_10_60,
		
		# slope + vol of vol
		"dlog_RV_5": dlog_RV_5,
		"dlog_RV_10": dlog_RV_10,
		"dlog_RV_22": dlog_RV_22,
		"vov_RV_5": vov_RV_5,
		"vov_RV_10": vov_RV_10,
		
		# returns
		"r1": r1,
		"r5": r5,
		"r22": r22,
		#"r60": r60,

		# volume
		"v_5": v_5,
		"v_22": v_22,
		#"v_60": v_60,
		
		# range
		"hl_range": hl_range,
		"hl_mean_5": hl_mean_5,
		"hl_mean_22": hl_mean_22,
	
		# calendar
		"days_to_opex": dte_opex,
        },
        index=stockdata.index
    	)
	
	# Drop NaN rows for auto-alignment	
	df_feat = df_feat.replace([np.inf, -np.inf], np.nan).dropna()
		
	return df_feat


def realized_vol(df, N=22, use_log=True):
	"""
	Close-to-close realized volatility over the *past* N trading days,
	"""
	close = df["Close"].to_numpy(dtype=float)
	
	r = np.full(len(close), np.nan)
	if use_log:
		r[1:] = np.log(close[1:] / close[:-1])
	else:
		r[1:] = (close[1:] / close[:-1]) - 1.0

	vol = np.full(len(close), np.nan)

	start = N
	for t in range(start, len(close)):
		s = t - N + 1
		window = r[s:t+1]
		vol[t] = np.std(window, ddof=1)

	return vol


def rolling_avg(arr, N):
	mean = 0

	for i in range(N - 1, len(arr)):
		mean += np.mean(arr[i-(N-1):i+1])

	mean /= (len(arr) - N + 1)

	return mean

def yz_rolling(df, N=5, min_history=20):
	'''
	yang-zhang volatility, uses OHLC instead of just close
	Previously used as a feature, not currently
	'''
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

	'''
	0.34 empirically the best value
	'''
	k = 0.34 / (1.34 + (N + 1) / (N - 1))



	for t in range(min_history + N - 1, n):
	#for t in range(min_history + N - 1, n, 5):

		# Window from s to t inclusive
		s = t - N + 1

		var_o = np.var(o[s:t+1], ddof=1)
		var_c = np.var(c[s:t+1], ddof=1)
		var_rs = np.mean(rs[s:t+1])

		yz[t] = np.sqrt(var_o + k * var_c + (1 - k) * var_rs)

	return yz

def volume_z(stockdata, min_history=90, N=5):
	'''
	Calculates a z score for volume.
	Min history by default is 90 days, so roughly compares
	to the previous quarters average
	'''
	volume = stockdata["Volume"]
	V = np.log1p(volume)
	
	wm = pd.Series(V, index=stockdata.index).rolling(N, min_periods=N).mean()
	mu = wm.rolling(min_history, min_periods=2).mean()
	sd = wm.rolling(min_history, min_periods=2).std(ddof=1)
	
	scores = (wm - mu) / sd

	return scores.to_numpy()


def days_to_opex(date: pd.Timestamp) -> int:
	'''
	Calculates days until operation expiration
	Closer to expiration --> more volatile
	'''

	date = pd.Timestamp(date).normalize()



	# cover current + next month comfortably
	schedule = nyse.schedule(start_date=date, end_date=date + pd.DateOffset(months=2))
	sessions = schedule.index.normalize()
	
	def snap_to_session(cal_day: pd.Timestamp) -> pd.Timestamp:
		cal_day = pd.Timestamp(cal_day).normalize()
		j = sessions.searchsorted(cal_day, side="right") - 1
		return sessions[0] if j < 0 else sessions[j]
	
	def next_opex_session(d: pd.Timestamp) -> pd.Timestamp:
		month, year = d.month, d.year
		cal_opex = find_third_friday(year, month).normalize()
		
		opex_sess = snap_to_session(cal_opex)
		
		if opex_sess <= d:
			nm = d + pd.DateOffset(months=1)
			cal_opex = find_third_friday(nm.year, nm.month).normalize()
			opex_sess = snap_to_session(cal_opex)
		return opex_sess

	opex_sess = next_opex_session(date)

	return int(((sessions > date) & (sessions <= opex_sess)).sum())


def find_third_friday(year, month):
	'''
	Used to suppliment days_to_opex
	Operations expire the third friday of each month
	Doesn't account for friday to not be a trading day
	'''

	cal = calendar.monthcalendar(year, month)
	fridays = []

	for week in cal:
		if week[calendar.FRIDAY] != 0:
			fridays.append(week[calendar.FRIDAY])

	assert len(fridays) >= 3

	return pd.Timestamp(year=year, month=month, day=fridays[2])

