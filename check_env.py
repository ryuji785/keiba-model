import sys
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import roc_auc_score

print("Python:", sys.version)
print("NumPy :", np.__version__)
print("Pandas:", pd.__version__)
print("XGBoost:", xgb.__version__)

y_true = [0, 0, 1, 1]
y_pred = [0.1, 0.4, 0.35, 0.8]
print("Sample AUC:", roc_auc_score(y_true, y_pred))
