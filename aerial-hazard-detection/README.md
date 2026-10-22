# AeroSafe — software-only project

All readings and map coordinates are simulated. This is an academic demonstration, not a real industrial safety system.

## macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python train_model.py
python app.py
```
Open http://127.0.0.1:5000

## Windows
```powershell
py -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python train_model.py
python app.py
```
Open http://127.0.0.1:5000

The dashboard simulates six channels, stores readings in SQLite, trains an SVM on synthetic data, draws an illustrative Leaflet map and shows a sensor history chart and alert table. Click **Simulate hazard** to generate elevated demo readings.

The displayed values, units, and model output are illustrative. A synthetic test score does not demonstrate real-world detection performance. External internet is needed for map tiles and chart assets.
