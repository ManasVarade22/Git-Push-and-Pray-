from pathlib import Path
import json
import joblib
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "data" / "Gas_Sensors_Measurements.csv"
MODEL_PATH = ROOT / "models" / "public_gas_dataset_svm.joblib"
REPORT_PATH = ROOT / "data" / "public_gas_dataset_evaluation.json"
FEATURES = ["MQ2", "MQ3", "MQ5", "MQ6", "MQ7", "MQ8", "MQ135"]
LABEL = "Gas"

def main():
    data = pd.read_csv(DATA_PATH)
    required = FEATURES + [LABEL, "Serial Number"]
    missing = [column for column in required if column not in data.columns]
    if missing:
        raise ValueError(f"Dataset is missing required columns: {missing}")
    if data[required].isna().any().any():
        raise ValueError("Dataset contains missing values in required columns.")

    # Hold out the last 20% of each class's serial sequence to reduce
    # leakage from neighboring sequential samples into the test set.
    train_parts, test_parts = [], []
    for _, group in data.groupby(LABEL, sort=True):
        group = group.sort_values("Serial Number")
        split_at = int(len(group) * 0.8)
        train_parts.append(group.iloc[:split_at])
        test_parts.append(group.iloc[split_at:])
    train = pd.concat(train_parts).sample(frac=1, random_state=42)
    test = pd.concat(test_parts).sample(frac=1, random_state=42)

    model = make_pipeline(
        StandardScaler(),
        SVC(kernel="rbf", class_weight="balanced", random_state=42)
    )
    model.fit(train[FEATURES], train[LABEL])
    predictions = model.predict(test[FEATURES])

    labels = sorted(data[LABEL].unique().tolist())
    report = {
        "dataset": "Gas_Sensors_Measurements.csv",
        "source_columns": FEATURES,
        "label_column": LABEL,
        "class_counts": {str(k): int(v) for k, v in data[LABEL].value_counts().sort_index().items()},
        "rows": int(len(data)),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "split_method": "within each class, first 80% of serial sequence for training and final 20% for testing",
        "accuracy": float(accuracy_score(test[LABEL], predictions)),
        "labels": labels,
        "confusion_matrix": confusion_matrix(test[LABEL], predictions, labels=labels).tolist(),
        "classification_report": classification_report(test[LABEL], predictions, labels=labels, output_dict=True, zero_division=0),
        "important_limitation": "Laboratory gas-class classification only; not a validated fire-risk model, not geolocated, and not compatible with the existing six-channel AeroSafe model."
    }
    joblib.dump(model, MODEL_PATH)
    REPORT_PATH.write_text(json.dumps(report, indent=2))
    print(f"Rows: {len(data)}")
    print(f"Training rows: {len(train)} | Test rows: {len(test)}")
    print(f"Features: {', '.join(FEATURES)}")
    print(f"Classes: {', '.join(labels)}")
    print(f"Holdout accuracy: {report['accuracy']:.4f}")
    print(classification_report(test[LABEL], predictions, labels=labels, zero_division=0))
    print(f"Saved separate model: {MODEL_PATH}")
    print(f"Saved evaluation report: {REPORT_PATH}")

if __name__ == "__main__":
    main()
