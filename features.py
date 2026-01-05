import numpy as np
from ingest import get_stockdata

def main():
	close, high, low, open_, volume = get_stockdata("TJX")
	yz_rolling(open_, high, low, close)

def yz_rolling(open_, high, low, close, N=5):
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

	for t in range(N, n):
		# k = 0.34 is empirically the best
		k = 0.34
		# Window from s to t inclusive
		s = t - N + 1

		var_o = np.var(o[s:t+1], ddof=1)
		var_c = np.var(c[s:t+1], ddof=1)
		var_rs = np.mean(rs[s:t+1])

		yz[t] = np.sqrt(var_o + k * var_c + (1 - k) * var_rs)

	print(yz)
	print(len(yz))



if __name__ == "__main__":
	main()