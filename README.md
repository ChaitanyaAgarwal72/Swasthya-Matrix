# 🏥 Swasthya-Matrix

**Real-Time Predictive Health Surveillance System for India**

Swasthya-Matrix is a Big Data pipeline that ingests a simulated daily stream of state-wise public health data from Kafka, runs a PySpark ML pipeline to predict district severity levels and compute vulnerability scores, and displays live results on a Streamlit dashboard.

---

## Architecture

```
Kafka Topic (health-stream)
        │  (71 records/day, 1 day/sec)
        ▼
PySpark Structured Streaming Consumer
        │  30-day tumbling window → sum → monthly-scale features
        │
        ├─ StandardScaler (KMeans features)
        ├─ RandomForest Classifier → Severity Level (0/1/2)
        ├─ StandardScaler (PCA features)
        ├─ PCA (k=1) → raw vulnerability score
        └─ MinMaxScaler (1–100, inverted) → Vulnerability Score
        │
        ├─ output_sink/latest_predictions.csv  (live dashboard feed)
        └─ HDFS /swasthya_data/predictions/    (batch archive)
                │
                ▼
        Streamlit Dashboard (app/dashboard.py)
```

---

## Project Structure

```
Swasthya-Matrix/
├── main.py                          # Orchestrator: starts all services
├── docker-compose.yml               # Zookeeper, Kafka, Hadoop (namenode + datanode)
├── hadoop.env                       # Hadoop config for Docker containers
├── setup_windows.py                 # One-time Windows setup: downloads winutils.exe
├── requirements.txt                 # Python dependencies
│
├── app/
│   └── dashboard.py                 # Streamlit real-time dashboard
│
├── data/
│   ├── cleaned_data_hmis.csv        # Training data (monthly, state-wise HMIS records)
│   └── synthetic_kafka_stream.csv   # Streaming data (daily = monthly/days_in_month)
│
├── models/
│   ├── swasthya_kmeans_scaler.model # StandardScaler for RF/KMeans features
│   ├── swasthya_kmeans.model        # KMeans (k=3) — used only during training
│   ├── swasthya_rf.model            # RandomForest (50 trees) — real-time severity predictor
│   ├── swasthya_pca_scaler.model    # StandardScaler for PCA features
│   ├── swasthya_pca_vulnerability.model  # PCA (k=1) — vulnerability dimension
│   └── swasthya_score_scaler.model  # MinMaxScaler (1–100) for final score
│
├── notebooks/
│   ├── Kmeans.ipynb                 # KMeans clustering + RF training
│   ├── pca.ipynb                    # PCA vulnerability model training
│   └── random_forest.ipynb         # RandomForest training details
│
├── scripts/
│   ├── kafka_producer.py            # Streams synthetic_kafka_stream.csv into Kafka
│   └── spark_consumer.py           # PySpark streaming consumer + ML pipeline
│
└── output_sink/
    └── latest_predictions.csv       # Written by Spark after each batch
```

---

## ML Pipeline

The pipeline runs inside `spark_consumer.py` on each 30-day windowed batch:

| Step | Model | Input → Output |
|---|---|---|
| 1 | `StandardScaler` | 16 health metrics + Region_Index → `features` |
| 2 | `RandomForestClassifier` | `features` → `Severity_Level` (0=Low, 1=Moderate, 2=High) |
| 3 | `StandardScaler` | 8 negative-outcome metrics → `scaled_negative_features` |
| 4 | `PCA (k=1)` | `scaled_negative_features` → `raw_pca_score` |
| 5 | `MinMaxScaler` | `raw_pca_score` → `Vulnerability_Score` (1–100, inverted) |

**Why RF instead of KMeans at runtime:** KMeans was used to label training data into 3 clusters. The RF was then trained on those labels — so at inference time the RF directly predicts the cluster/severity from raw features without needing to re-cluster.

**Why PCA score is inverted:** PCA PC1 captures variance driven by absolute disease count magnitude. Larger-population states with higher absolute burden get negative PC1 values. Inverting (`101 - MinMaxScaled`) maps high disease burden → high vulnerability score (closer to 100).

---

## Prerequisites

- **Python 3.10+**
- **Docker Desktop** (running)
- **Java 11+** on PATH (required by PySpark)
- **Windows only:** run `setup_windows.py` once to install `winutils.exe`

---

## Setup & Run

### 1. Install dependencies
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Windows PySpark patch (first time only)
```bash
python setup_windows.py
```

### 3. Run the full pipeline
```bash
python main.py
```

`main.py` will:
1. Start Docker containers (Zookeeper, Kafka, Hadoop)
2. Reset the Kafka topic (clears previous run's data)
3. Create HDFS directories
4. Start the PySpark consumer and wait for models to load
5. Start the Kafka producer (streams 180 days of data, 1 day/sec)
6. Launch the Streamlit dashboard at `http://localhost:8501`
7. Print "Data stream complete" when all data is sent — dashboard stays live
8. Press **Ctrl+C** to gracefully shut everything down

---

## Dashboard

The Streamlit dashboard at `http://localhost:8501` shows:

- **Metrics bar:** total states monitored, high-risk region count, max vulnerability score
- **Alert banner:** lists all High Risk regions when detected
- **Regional Vulnerability Bar Chart:** all states ranked by score
- **Live Predictions Table:** Location, colour-coded Severity (🔴 High / 🟠 Moderate / 🟢 Low), Vulnerability Score
- **Risk Level Distribution:** count of states per severity bucket
- **Top 10 Most Vulnerable Regions:** focused bar chart

Dashboard refreshes every second; stable predictions appear after ~30 seconds per window (one simulated month).

---

## Hadoop / HDFS

The Hadoop NameNode web UI is available at **`http://localhost:9870`** while Docker is running.

- Browse files: Utilities → Browse the file system → `/swasthya_data/predictions/`
- Each batch is written as a separate CSV partition under `batch_<id>/`

---

## Data Notes

- **Training data** (`cleaned_data_hmis.csv`): real HMIS monthly aggregates per state/region
- **Streaming data** (`synthetic_kafka_stream.csv`): synthetically generated daily records where each value = `monthly_value / days_in_month` (integer). Summing a full 30-day window reproduces the monthly magnitude that the models were trained on.
