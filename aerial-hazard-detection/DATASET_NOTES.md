# AeroSafe sensor-data provenance

## Current application status
The current `simulator.py` and `train_model.py` generate synthetic readings and synthetic labels. The map coordinates in `app.py` and `network_injector.py` are also generated around the Pune map centre. These are not real field measurements and are not tied to real industrial sites or actual water-body conditions.

## Public dataset candidate
MultimodalGasData (Mendeley Data, CC BY 4.0) includes simultaneous readings from MQ-2, MQ-3, MQ-5, MQ-6, MQ-7, MQ-8 and MQ-135 sensors, with classes No Gas, Perfume, Smoke, and Perfume+Smoke:
https://data.mendeley.com/datasets/zkwgkjkjn9/2
DOI: 10.17632/zkwgkjkjn9.2

This is a closer sensor-family match than many gas datasets, but it does not have the exact AeroSafe channel set (MQ-4 and MQ-9 are missing; MQ-2, MQ-5 and MQ-6 are extra). Its class labels describe experimental gas conditions, not a verified fire-risk label. It also has no geographic coordinates for placing observations on the Pune map.

The UCI Gas Sensor Array Drift dataset is another real laboratory sensor benchmark (16 sensors, six gases, 13,910 samples), but its feature representation is derived sensor-response features and is not directly interchangeable with the six MQ concentration values in AeroSafe:
https://archive.ics.uci.edu/dataset/224/gas

## Safe integration plan
1. Download the dataset files from the source page and preserve its attribution/license.
2. Inspect its file format and sensor units, then map only matching channels; do not fabricate missing MQ-4 or MQ-9 values and call them real.
3. Display imported samples as **laboratory dataset examples**, separate from the live map. Without GPS or site measurements, these samples cannot be honestly plotted as real-world geographic hazard points.
4. If training a model on these samples, define labels and feature conversions first, then evaluate against held-out real measurements. Do not treat synthetic model accuracy as evidence of real-world fire detection performance.

## Map changes in this build
The map now clusters nearby points at lower zoom levels, uses smaller lower-opacity markers, has a clearer legend, and states that the locations/readings are simulated. No model, thresholds, UDP protocol, database schema, or alert logic was changed.
