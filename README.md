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
        │  Calendar-month aggregation (28/29/30/31 days) → monthly-scale features
        │
        ├─ StandardScaler (KMeans features)
        ├─ RandomForest Classifier → Severity Level (0/1/2)
        ├─ StandardScaler (PCA features)
        ├─ PCA (k=1) → raw vulnerability score
        └─ MinMaxScaler (1–100, inverted) → Vulnerability Score
        │
        ├─ output_sink/latest_predictions.csv   (live dashboard feed)
        └─ HDFS /swasthya_data/predictions/     (batch archive)
                │
                ▼
        Streamlit Dashboard (app/dashboard.py)
                │
                ├─ High Risk detection (Severity = 2)
                ├─ Metric lookup from data/cleaned_data_hmis.csv
                ├─ Gemini API call via scripts/llm_insights.py
                └─ GenAI Emergency Insights panel (cached 1 hour)
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
│   ├── spark_consumer.py            # PySpark streaming consumer + ML pipeline
│   └── llm_insights.py              # Gemini client wrapper for emergency insights
│
└── output_sink/
        └── latest_predictions.csv       # Recreated by Spark after startup cleanup
```

---

## ML Pipeline

The pipeline runs inside `spark_consumer.py` on each calendar-month batch:

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
1. Clear `output_sink/` to start with a fresh run
2. Start Docker containers (Zookeeper, Kafka, Hadoop)
3. Reset the Kafka topic (clears previous run's data)
4. Create HDFS directories
5. Start the PySpark consumer and wait for models to load
6. Start the Kafka producer (streams 180 days of data, 1 day/sec)
7. Launch the Streamlit dashboard at `http://localhost:8501`
8. Print "Data stream complete" when all data is sent; dashboard stays live
9. Press **Ctrl+C** to gracefully shut everything down

---

## Dashboard

The Streamlit dashboard at `http://localhost:8501` shows:

- **Metrics bar:** total states monitored, high-risk region count, max vulnerability score
- **Alert banner:** lists all High Risk regions when detected
- **Regional Vulnerability Bar Chart:** all states ranked by score
- **Live Predictions Table:** Location, colour-coded Severity (🔴 High / 🟠 Moderate / 🟢 Low), Vulnerability Score
- **Risk Level Distribution:** count of states per severity bucket
- **Top 10 Most Vulnerable Regions:** focused bar chart
- **GenAI Emergency Insights:** per-region emergency recommendations for High Risk locations

Dashboard refreshes every second; stable predictions appear after enough daily records accumulate for a month (one simulated month).

---

## Hadoop / HDFS

The Hadoop NameNode web UI is available at **`http://localhost:9870`** while Docker is running.

- Browse files: Utilities → Browse the file system → `/swasthya_data/predictions/`
- Each batch is written as a separate CSV partition under `batch_<id>/`

---

## 🤖 GenAI Emergency Insights

### Overview

The dashboard includes AI-powered insights for high-risk regions using **Google's Gemini API**. When any region is flagged as High Risk (Severity_Level = 2), an automated analysis is triggered to:

1. Extract live health metrics from that region
2. Send them to Gemini API with a prompt asking for actionable emergency recommendations
3. Display the AI-generated insights directly in the dashboard

### Features

- **Automated Analysis:** Triggered only when High Risk regions are detected
- **Real Health Data:** Uses actual HMIS metrics (ANC, immunization, maternal deaths, etc.)
- **Actionable Insights:** 2-sentence emergency response recommendations pinpointing root causes
- **Smart Caching:** Results cached for 1 hour to reduce API calls
- **Graceful Fallback:** If API is unavailable, displays a user-friendly message

### Configuration

1. **Get a Gemini API key:**
   - Visit [Google AI Studio](https://aistudio.google.com/apikey)
   - Click "Get API Key" → "Create API Key in new project"
   - Copy your key

2. **Add to `.env` file:**
   ```
   GEMINI_API_KEY=your_actual_gemini_api_key_here
   ```

3. The dashboard will automatically load the API key from `.env` on startup

### Example Output

When a high-risk region is detected, you'll see:

```
🤖 GenAI Emergency Insights

ℹ️ 2019-04-14 05:30:00 | A & N Islands (Rural) (Score: 85.42)

High rates of infant sepsis (2 cases) and low institutional delivery 
coverage (307/1000) indicate inadequate perinatal care infrastructure. 
Recommend immediate deployment of maternal health workers and sepsis 
screening protocols at primary health centers.
```

### Health Metrics Analyzed

The AI analyzes the following 16 key health indicators:
- **Maternal Health:** ANC Registered, Institutional Delivery, Maternal Death (Bleeding)
- **Child Health:** Child Diarrhea, Child TB, Child Malaria, Immunization MR, Infant Death (Sepsis), Low Birth Weight
- **Reproductive Health:** C-Section, Early Breastfeeding, PW Hypertension, Severe Anaemia Treated, Condoms Distributed
- **Adult Health:** Adult Death (TB), Adult Suicide

### How It Works

1. After each calendar-month prediction batch, the ML pipeline identifies High Risk regions
2. The dashboard loads source data from `data/cleaned_data_hmis.csv`
3. For each high-risk region, metrics are matched by State + Region Type
4. Metrics are formatted and sent to Gemini API with a public health analysis prompt
5. AI response is cached and displayed in an info card

### Troubleshooting

| Issue | Solution |
|-------|----------|
| "API Key not found" | Ensure `.env` file exists with valid `GEMINI_API_KEY` |
| "AI Insight temporarily unavailable" | API rate limit exceeded or network issue; try again in a moment |
| No metrics displayed | Source data (`data/cleaned_data_hmis.csv`) may be missing or region type mismatch |

---

## Data Notes

- **Training data** (`cleaned_data_hmis.csv`): real HMIS monthly aggregates per state/region
- **Streaming data** (`synthetic_kafka_stream.csv`): synthetically generated daily records where each value = `monthly_value / days_in_month` (integer). Summing all daily records within each calendar month reproduces the monthly magnitude that the models were trained on.
