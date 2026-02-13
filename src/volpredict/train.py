import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import Ridge, QuantileRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor

def train_ridge(X, y, alpha=10):
	
	model = Pipeline([
		("scaler", StandardScaler()),
		("ridge", Ridge(alpha=alpha))
		])
        
	model.fit(X, y)	
	
	return model


def train_hgb(X, y, loss="absolute_error", max_depth=3, learning_rate=0.05, max_iter=500):
	# No scaling needed for tree models
	model = HistGradientBoostingRegressor(
	loss="squared_error",
	max_depth=3,
	learning_rate=0.05,
	max_iter=2000,
	early_stopping=True,
	n_iter_no_change=50,
	l2_regularization=1.0,
	random_state=0)
	model.fit(X, y)
	return model

def alpha_cv(X_train, y_train, alphas=None, splits=5, gap=22):
	if alphas is None:
		alphas = np.logspace(-2, 4, 25)
	
	tscv = TimeSeriesSplit(n_splits=splits, gap=gap)
	
	best_a, best_mse = None, np.inf
	for a in alphas:
		mses = []
		for tr, va in tscv.split(X_train):
			model = Pipeline([
				("scaler", StandardScaler()),
				("ridge", Ridge(alpha=a))
			])

			X_tr, y_tr = X_train[tr], y_train[tr]
			X_va, y_va = X_train[va], y_train[va]

			model.fit(X_tr, y_tr)
			y_hat = model.predict(X_va)
			mses.append(mean_squared_error(y_va, y_hat))
			
		cv_mse = float(np.mean(mses))
		if cv_mse < best_mse:
			best_a, best_mse = a, cv_mse

	return best_a
