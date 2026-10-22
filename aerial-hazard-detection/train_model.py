
from pathlib import Path

import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, classification_report

BASE = Path(__file__).resolve().parent
FEATURE_COUNT = 56

NORMAL_LOW = np.array([0.05, 100, 2, 20, 5, 50], dtype=float)
NORMAL_HIGH = np.array([0.8, 700, 20, 180, 30, 250], dtype=float)

HAZARD_LOW = np.array([1.1, 1300, 85, 500, 80, 500], dtype=float)
HAZARD_HIGH = np.array([2.5, 3000, 250, 1500, 200, 1200], dtype=float)


def dataset(n=6000, seed=42):
    rng = np.random.default_rng(seed)
    X = np.zeros((2 * n, FEATURE_COUNT), dtype=float)
    y = np.concatenate([
        np.zeros(n, dtype=int),
        np.ones(n, dtype=int)
    ])

    for label, start in ((0, 0), (1, n)):
        if label == 0:
            low, high = NORMAL_LOW, NORMAL_HIGH
            temp, wind = (10, 30), (0, 5)
        else:
            low, high = HAZARD_LOW, HAZARD_HIGH
            temp, wind = (25, 50), (5, 20)

        readings = rng.uniform(low, high, size=(n, 9, 6))
        confidence = rng.uniform(0.35, 1.0, size=(n, 9, 1))

        X[start:start + n, :54] = (
            readings * confidence
        ).reshape(n, 54)

        X[start:start + n, 54] = rng.uniform(*temp, size=n)
        X[start:start + n, 55] = rng.uniform(*wind, size=n)

    indices = rng.permutation(len(y))
    return X[indices], y[indices]


def build_model():
    return make_pipeline(
        StandardScaler(),
        SVC(
            probability=True,
            class_weight="balanced",
            random_state=42
        )
    )


def train():
    X, y = dataset()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = build_model()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    print("56-feature synthetic test accuracy:",
          round(accuracy_score(y_test, predictions), 3))
    print(classification_report(y_test, predictions))

    model_dir = BASE / "models"
    model_dir.mkdir(exist_ok=True)
    joblib.dump(model, model_dir / "svm_model.joblib")
    print("Saved 56-feature SVM model.")

    return model


def train_legacy():
    rng = np.random.default_rng(43)
    n = 6000

    normal = rng.uniform(NORMAL_LOW, NORMAL_HIGH, size=(n, 6))
    hazard = rng.uniform(HAZARD_LOW, HAZARD_HIGH, size=(n, 6))

    X = np.vstack((normal, hazard))
    y = np.concatenate((np.zeros(n, dtype=int), np.ones(n, dtype=int)))

    indices = rng.permutation(len(y))
    X, y = X[indices], y[indices]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=43, stratify=y
    )

    model = build_model()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    print("6-feature synthetic test accuracy:",
          round(accuracy_score(y_test, predictions), 3))
    print(classification_report(y_test, predictions))

    model_dir = BASE / "models"
    model_dir.mkdir(exist_ok=True)
    joblib.dump(model, model_dir / "legacy_svm_model.joblib")
    print("Saved 6-feature legacy SVM model.")

    return model


if __name__ == "__main__":
    train()
    train_legacy()
