import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score, classification_report, r2_score, mean_squared_error

# --- Load your dataset from CSV (you need to export from Snowflake first) ---
df = pd.read_csv("weather_data.csv")  

df = df.dropna()

# --- Define regression targets and classification label ---
reg_targets = [
    "AVGTEMP_C", "MAXTEMP_C", "MINTEMP_C",
    "TOTALPRECIP_MM", "DAILY_CHANCE_OF_RAIN",
    "AVGHUMIDITY", "MAXWIND_KPH", "MAXWIND_MPH"
]
cls_multi_target = "CONDITION"

# --- Encode REGION ---
region_encoder = LabelEncoder()
df["REGION_ENC"] = region_encoder.fit_transform(df["REGION"])

# --- REGRESSION MODEL ---
def train_model_multi_regression(df):
    feature_cols = [col for col in df.columns if col not in reg_targets + ["REGION"]+ [cls_multi_target, "NEW_DATE"]]
    X = df[feature_cols]
    y = df[reg_targets]

    print("📊 Features used for regression:", feature_cols)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = MultiOutputRegressor(
        RandomForestRegressor(n_estimators=100, random_state=42)
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    print("📊 Regression Metrics:")
    for i, col in enumerate(reg_targets):
        mse = mean_squared_error(y_test[col], y_pred[:, i])
        r2 = r2_score(y_test[col], y_pred[:, i])
        print(f"{col}: MSE={mse:.2f}, R2={r2:.2f}")

    joblib.dump(model, "multioutput_regressor.pkl")

# --- CLASSIFICATION MODEL ---
def train_model_classification(df):
    le = LabelEncoder()
    df["CONDITION_ENC"] = le.fit_transform(df["CONDITION"])

    feature_cols = reg_targets + ["REGION_ENC"]
    X = df[feature_cols]
    y = df["CONDITION_ENC"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, max_depth=20, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    print("📊 Classification Metrics:")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(classification_report(
        y_test, y_pred,
        labels=le.transform(le.classes_),
        target_names=le.classes_,
        zero_division=0
    ))

    joblib.dump(model, "weather_condition_model.pkl")
    joblib.dump(le, "condition_label_encoder.pkl")
    joblib.dump(region_encoder, "region_label_encoder.pkl")

# --- Run Training ---
train_model_multi_regression(df)
train_model_classification(df)
