# Public gas-sensor dataset integration

## What was added
- `data/Gas_Sensors_Measurements.csv`: the supplied 6,400-row dataset.
- `evaluate_public_gas_dataset.py`: trains and evaluates a separate SVM classifier.
- `models/public_gas_dataset_svm.joblib`: the new model artifact; it does not overwrite either existing AeroSafe model.
- `data/public_gas_dataset_evaluation.json`: detailed evaluation metrics.

## Run the evaluation
From the `aerial-hazard-detection` directory:

```bash
python3 -m pip install -r requirements.txt
python3 evaluate_public_gas_dataset.py
```

The script uses MQ2, MQ3, MQ5, MQ6, MQ7, MQ8, and MQ135 to classify the dataset labels `NoGas`, `Perfume`, `Smoke`, and `Mixture`. It holds out the final 20% of each class's serial sequence for testing, rather than randomly mixing adjacent sequential samples across train/test.

## Initial result
On the supplied CSV, the script reported 91.33% accuracy on 1,280 held-out samples. Per-class results are saved in `data/public_gas_dataset_evaluation.json`. This is an initial dataset-specific result, not a guarantee of performance on new sensors or environments.

## Important limitations
- These are laboratory gas-class labels, not verified fire/hazard labels.
- The dataset has no GPS coordinates.
- It has MQ2/MQ3/MQ5/MQ6/MQ7/MQ8/MQ135, while the original AeroSafe six-channel model expects MQ3/MQ4/MQ7/MQ8/MQ9/MQ135. The channel mismatch means this model must not be substituted into the existing live-grid inference pipeline.
- The dashboard, database schema, UDP pipeline, existing models, and risk thresholds have not been changed.
