import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt

from volpredict.features import const_matrix
from volpredict.train import train_ridge, alpha_cv, train_hgb
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score 
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor, ExtraTreesRegressor

from volpredict.paths import RAW_DIR, CONFIG_DIR


def main():
	tw = pd.read_csv(CONFIG_DIR / "constituents.csv")
	
	names = tw["Symbol"].to_numpy()
	

	n_ok = 0
	results = []
	impr = []
	for t in names:
		out = run(t)
		if out is not None:
			results.append(out)
			rmse_test = out["rmse"]
			rmse_har = out["rmse_har"]
			impr.append((rmse_har - rmse_test) / rmse_har)
			
	
	if not results:
		print("no results")
		return

	n_ok = len(results)

	avg_rmse_in = np.mean([r["rmse_in"] for r in results])
	avg_rmse = np.mean([r["rmse"] for r in results])
	avg_har = np.mean([r["rmse_har"] for r in results])

	pct_pos_har = np.mean([r > 0 for r in impr])

	impr = np.array(impr) * 100
	median = np.percentile(impr, 50)
	p10, p25, p75, p90 = np.percentile(impr, [10, 25, 75, 90])

	print(f"10th Percentile: {p10:.2f}%")
	print(f"25th Percentile: {p25:.2f}%")
	print(f"50th Percentile: {median:.2f}%")
	print(f"75th Percentile: {p75:.2f}%")
	print(f"90th Percentile: {p90:.2f}%")
	print(f"Averaged over {n_ok} stocks")
	print(f"RMSE: train={avg_rmse_in:.4f}  test={avg_rmse:.4f}  HAR={avg_har:.4f}")
	print(f"Win Percentage: {pct_pos_har:.1%}")




def run(ticker: str):
	data = const_matrix(ticker, horizon=5)

	
	if data is None or len(data) < 200:
		return None
	
	har_cols = [c for c in data.columns if c in ("log_RV_1", "log_RV_5", "log_RV_22")]
	X_har = data[har_cols].to_numpy()
	y_har = data["y"].to_numpy()

	
	feature_cols = [c for c in data.columns if c not in ("y")]
	X = data[feature_cols].to_numpy()
	y = data["y"].to_numpy()  # log-vol

	N = len(y)
	train_end = int(0.8 * N)

	X_train, y_train = X[:train_end], y[:train_end]
	X_test, y_test = X[train_end:], y[train_end:]

	X_har_train, y_har_train = X_har[:train_end], y_har[:train_end]
	X_har_test, y_har_test = X_har[train_end:], y_har[train_end:]


	# Make testing more accurate, toss out overlapping days
	
	idx = np.arange(0, len(y_test), 5)
	X_test = X_test[idx]
	y_test = y_test[idx]

	X_har_test = X_har_test[idx]
	y_har_test = y_har_test[idx]

	# Optimize my model
	alpha = alpha_cv(X_train, y_train)
	model = train_ridge(X_train, y_train, alpha=alpha)

	#model = ExtraTreesRegressor(
	#n_estimators=1500,
	#max_depth=12,
	#min_samples_leaf=10,
	#max_features=0.5,
	#bootstrap=False,
	#n_jobs=-1,
	#random_state=42,
	#)
	#model.fit(X_train, y_train)
	# Optimize har baseline
	har_alpha = alpha_cv(X_har_train, y_har_train)
	har_model = train_ridge(X_har_train, y_har_train, alpha=har_alpha)

	# Predict on in and out of sample
	y_hat_tr = model.predict(X_train)
	y_hat_te = model.predict(X_test)
	
	y_har_hat_tr = har_model.predict(X_har_train)
	y_har_hat_te = har_model.predict(X_har_test)


	# --- model + HAR baseline evaluation (HAR IS THE ONLY BASELINE) ---
	E_in  = mean_squared_error(y_train, y_hat_tr)
	E_out = mean_squared_error(y_test,  y_hat_te)
	E_har = mean_squared_error(y_test,  y_har_hat_te)

	rmse_in  = float(np.sqrt(E_in))
	rmse_out = float(np.sqrt(E_out))
	rmse_har = float(np.sqrt(E_har))

	skill_vs_har = float(1.0 - E_out / E_har)

	# correlations (optional, for intuition)
	corr_y = float(np.corrcoef(y_hat_te, y_test)[0, 1]) if len(y_test) > 2 else float("nan")


	vol_actual = np.exp(y_hat_te)
	vol2 = vol_actual ** 2
	QLIKE = np.log(vol2) + (np.exp(y_test) ** 2) / vol2	
	
	QLIKE_avg = np.mean(QLIKE)
	
	vol_actual = np.exp(y_har_hat_te)
	vol2 = vol_actual ** 2
	QLIKE = np.log(vol2) + (np.exp(y_test) ** 2) / vol2	
	QLIKE_avg_base = np.mean(QLIKE)

	print(f"QLIKE = {QLIKE_avg}")
	print(f"Baseline QLIKE = {QLIKE_avg_base}")
	print(f"{ticker} n_test={len(y_test)}")
	print(f"RMSE train={rmse_in:.4f}  test={rmse_out:.4f}")
	print(f"Baseline (HAR) RMSE={rmse_har:.4f}  skill_vs_HAR={skill_vs_har:.3f}  corr(pred,y)={corr_y:.3f}")

	# optional plot (set do_plot=True in run signature if you want)
	x = np.arange(len(y_test))
	plt.figure()
	plt.plot(x, np.exp(y_test), label="Actual")
	plt.plot(x, np.exp(y_hat_te), label="Model")
	plt.plot(x, np.exp(y_har_hat_te), label="HAR baseline")
	plt.title(f"{ticker} | skill_vs_HAR={skill_vs_har:.3f}")
	plt.xlabel("test step (every 5th day)")
	plt.ylabel("Vol")
	plt.legend()
	plt.tight_layout()
	plt.show()

	return {
		"ticker": ticker,
		"n_test": int(len(y_test)),
		"rmse_in": rmse_in,
		"rmse": rmse_out,
		"rmse_har": rmse_har,
		"skill_har": skill_vs_har,
		}

	
if __name__ == "__main__":
	main()
