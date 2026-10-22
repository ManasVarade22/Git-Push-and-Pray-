from pathlib import Path
import sqlite3, random, joblib, numpy as np, json
import psycopg
from flask import Flask, jsonify, render_template, request
from simulator import SENSORS, generate
from train_model import train
BASE=Path(__file__).resolve().parent; DB=BASE/"hazard.db"; MP=BASE/"models/svm_model.joblib"
LEGACY_MP=BASE/"models/legacy_svm_model.joblib"
app=Flask(__name__)
def db():
 c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c
def init():
 with db() as c:
  c.execute("""CREATE TABLE IF NOT EXISTS readings(id INTEGER PRIMARY KEY,timestamp TEXT,mq3 REAL,mq4 REAL,mq7 REAL,mq8 REAL,mq9 REAL,mq135 REAL,risk TEXT,score REAL,lat REAL,lon REAL,simulated INTEGER)""")
def model():
 if not MP.exists(): return train()
 return joblib.load(MP)
MODEL=model()
LEGACY_MODEL=joblib.load(LEGACY_MP)
init()
def classify(v):
 p=LEGACY_MODEL.predict_proba(np.array(v).reshape(1,-1))[0]
 classes=list(LEGACY_MODEL.classes_)
 score=float(p[classes.index(1)])*100
 return ("HIGH" if score>=75 else "WARNING" if score>=40 else "NORMAL",round(score,1))
@app.route("/")
def home(): return render_template("index.html")
@app.route("/api/readings/latest")
def latest():
 r=generate(request.args.get("scenario")=="hazard"); risk,score=classify(r["values"])
 lat=18.5204+random.uniform(-.0015,.0015); lon=73.8567+random.uniform(-.0015,.0015)
 with db() as c: c.execute("INSERT INTO readings(timestamp,mq3,mq4,mq7,mq8,mq9,mq135,risk,score,lat,lon,simulated) VALUES(?,?,?,?,?,?,?,?,?,?,?,1)",(r["timestamp"],*r["values"],risk,score,lat,lon))
 r.update(risk=risk,score=score,lat=lat,lon=lon,simulated=True)
 r["sensor_details"]=[{"id":k,"gas":SENSORS[k][0],"unit":SENSORS[k][1],"value":v} for k,v in zip(r["sensors"],r["values"])]
 return jsonify(r)
@app.route("/api/readings/history")
def history():
 with db() as c: rows=c.execute("SELECT * FROM readings ORDER BY id DESC LIMIT 50").fetchall()
 return jsonify([dict(x) for x in reversed(rows)])
@app.route("/api/alerts")
def alerts():
 with db() as c: rows=c.execute("SELECT timestamp,risk,score,mq4,mq7,mq8 FROM readings WHERE risk!='NORMAL' ORDER BY id DESC LIMIT 20").fetchall()
 return jsonify([dict(x) for x in rows])
@app.route("/api/status")
def status():
 with db() as c: n=c.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
 return jsonify(reading_count=n,sensor_count=6,data_mode="SIMULATED")



@app.route("/api/grid/live")
def live_grid():
    import os
    from math import cos, radians

    database_url = os.getenv(
        "DATABASE_URL",
        "dbname=aerosafe user=saksham"
    )

    origin_lat = 18.5204
    origin_lon = 73.8567
    cell_size_metres = 10.0
    metres_per_degree_lat = 111320.0
    metres_per_degree_lon = (
        111320.0 * cos(radians(origin_lat))
    )

    with psycopg.connect(database_url) as conn:
        rows = conn.execute("""
            SELECT DISTINCT ON (cell_x, cell_y)
                cell_x, cell_y,
                sensor_values, risk_score, risk_level,
                confidence, received_at
            FROM sensor_readings
            WHERE received_at >= NOW() - INTERVAL '20 seconds'
            ORDER BY cell_x, cell_y, received_at DESC
        """).fetchall()

    result = []

    for row in rows:
        x, y = row[0], row[1]

        latitude = origin_lat + (
            (y + 0.5) * cell_size_metres / metres_per_degree_lat
        )
        longitude = origin_lon + (
            (x + 0.5) * cell_size_metres / metres_per_degree_lon
        )

        result.append({
            "cell_id": f"{x}:{y}",
            "x": x,
            "y": y,
            "latitude": latitude,
            "longitude": longitude,
            "sensors": row[2],
            "risk_score": row[3],
            "risk_level": row[4] or "UNKNOWN",
            "confidence": row[5],
            "last_received": (
                row[6].isoformat() if row[6] else None
            )
        })

    return jsonify(result)



PUBLIC_DATA_PATH = BASE / "data" / "Gas_Sensors_Measurements.csv"
PUBLIC_MODEL_PATH = BASE / "models" / "public_gas_dataset_svm.joblib"
PUBLIC_REPORT_PATH = BASE / "data" / "public_gas_dataset_evaluation.json"
PUBLIC_FEATURES = ["MQ2", "MQ3", "MQ5", "MQ6", "MQ7", "MQ8", "MQ135"]

@app.route("/api/public-dataset/summary")
def public_dataset_summary():
    import csv
    with PUBLIC_DATA_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        counts = {}
        sample = []
        total = 0
        for row in reader:
            total += 1
            label = row.get("Gas", "Unknown")
            counts[label] = counts.get(label, 0) + 1
            if len(sample) < 8:
                sample.append({
                    "serial": row.get("Serial Number", ""),
                    "gas": label,
                    "readings": {key: row.get(key, "") for key in PUBLIC_FEATURES}
                })
    report = json.loads(PUBLIC_REPORT_PATH.read_text()) if PUBLIC_REPORT_PATH.exists() else {}
    return jsonify({
        "dataset": "Gas Sensors Measurements",
        "rows": total,
        "features": PUBLIC_FEATURES,
        "label_column": "Gas",
        "class_counts": counts,
        "sample": sample,
        "accuracy": report.get("accuracy"),
        "test_rows": report.get("test_rows"),
        "report": report,
        "limitation": "Laboratory gas-class classification only. Not a validated fire-risk model, not geolocated, and separate from the simulated live-grid model."
    })

@app.route("/api/public-dataset/predict", methods=["POST"])
def public_dataset_predict():
    payload = request.get_json(silent=True) or {}
    try:
        values = [float(payload[name]) for name in PUBLIC_FEATURES]
    except (KeyError, TypeError, ValueError):
        return jsonify(error="Provide numeric values for all seven sensor channels."), 400
    if not all(np.isfinite(value) for value in values):
        return jsonify(error="All sensor readings must be finite numbers."), 400
    if not PUBLIC_MODEL_PATH.exists():
        return jsonify(error="Public dataset model not found. Run evaluate_public_gas_dataset.py first."), 503
    public_model = joblib.load(PUBLIC_MODEL_PATH)
    prediction = str(public_model.predict(np.array(values).reshape(1, -1))[0])
    probabilities = {}
    if hasattr(public_model, "predict_proba"):
        probs = public_model.predict_proba(np.array(values).reshape(1, -1))[0]
        probabilities = {str(label): round(float(prob) * 100, 2) for label, prob in zip(public_model.classes_, probs)}
    return jsonify({
        "prediction": prediction,
        "probabilities": probabilities,
        "features": dict(zip(PUBLIC_FEATURES, values)),
        "notice": "This predicts a dataset gas class, not fire risk or real-world safety status."
    })

if __name__=="__main__": app.run(debug=True)
