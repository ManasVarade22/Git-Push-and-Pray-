# P05 — SVM and asynchronous weather fusion

## Original model inspection

The GitHub repository was inspected recursively and searched for SVM, SVC,
scaler, and training-schema artifacts. It contains no executable SVM artifact,
training dataset, preprocessing pipeline, or expected feature-order manifest.
The project presentation is not a trained model. Consequently, no original
classifier, scaling values, training order, accuracy, or industrial-fire
performance claim can be verified or loaded.

The streaming grid's `stream-grid-v1` schema is used only as the explicit
feature contract for the synthetic demo in this change. It is not represented
as the missing original training schema. Sensor values and the synthetic rule
are demonstration inputs, not field measurements or fire labels.

## Synthetic demonstration model

`synthetic_demo_svm.py` trains a separate deterministic scikit-learn pipeline
at runtime: `StandardScaler` followed by an RBF `SVC` with sigmoid probability
calibration through `CalibratedClassifierCV`. Its
generated labels use a simple synthetic rule based on center and neighboring
channel counts. The model adapter validates the exact feature order, rejects
missing values, and returns the probability for class `1`. Risk inference is
performed by the grid synchronously for each accepted frame whose complete
feature vector is available; it does not wait for or invoke PostgreSQL.

The demonstration model is not trained on real sensor readings, has no
validated accuracy, and must not be used for operational fire decisions.
Supply a real model artifact together with its fitted preprocessing,
positive-class meaning, and exact feature-order manifest before replacing the
demo. The repository has no original store-then-predict pipeline to preserve;
this change does not invent one.

## Weather refresh

`AsyncWeatherRefresh` takes an injected async provider, avoiding a hard-coded
weather API or secret. Refresh calls have a bounded timeout. Successful data is
cached with its provider timestamp and copied into the grid's weather cache;
failed or timed-out refreshes preserve the last good sample and expose the
error. Snapshots report `fresh`, `stale`, or `unavailable` based on a
configurable maximum age. The grid independently suppresses stale weather
features, leaving them unknown rather than substituting zero.

Install the SVM dependency with `pip install -r requirements.txt`.

