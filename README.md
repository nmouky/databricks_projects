# Turbine Data Lakehouse Pipeline

## Table of Contents

- [Introduction](#introduction)
- [Architecture](#architecture)
- [ADLS Data Layout](#adls-data-layout)
- [Repository Layout](#repository-layout)
- [Notebooks](#notebooks)
  - [Bronze Tests](#bronze-tests)
  - [Bronze Ingestion](#bronze-ingestion)
  - [Silver Transformation](#silver-transformation)
  - [Gold Analytics](#gold-analytics)
    - [Daily Power Summaries](#daily-power-summaries)
    - [Anomaly Detection](#anomaly-detection)
    - [Gold Outputs](#gold-outputs)
- [Job Orchestration](#job-orchestration)
- [Running the Project](#running-the-project)
- [Design Considerations](#design-considerations)
- [Future Improvements](#future-improvements)
- [Key Notes*](#key-notes)

## Introduction

This project implements a Databricks medallion-style pipeline for wind-turbine telemetry. It ingests CSV files from Azure Data Lake Storage (ADLS), stores raw records in Bronze, validates and standardizes data in Silver, and produces Gold-level operational summaries and anomaly outputs.

## Architecture

```text
ADLS Gen2 Landing Zone
└── data_group_*.csv
    │
    ▼
Bronze Validation Tests
    │
    ▼
Bronze Ingestion and Delta MERGE
└── interviews_dev.bronze.turbine_raw
    │
    ▼
Silver Validation and Standardisation
└── interviews_dev.silver.turbine_clean
    │
    ▼
Gold Analytics
├── turbine_daily_summary
├── turbine_anomalies
└── Analysis-ready DataFrames and Visualisations
```

## ADLS Data Layout

The ingestion notebook reads turbine CSV files from the configured Azure Data Lake Storage Gen2 landing path:

```text
abfss://raw@collibri.dfs.core.windows.net/landing/
```

Files are selected using the pattern:

```text
data_group_*.csv
```

The source records contain turbine telemetry such as:

| Source column | Description |
|---|---|
| `timestamp` | Timestamp at which the measurement was recorded |
| `turbine_id` | Turbine identifier |
| `wind_speed` | Recorded wind speed |
| `wind_direction` | Recorded wind direction |
| `power_output` | Turbine power output in MW |

During Bronze ingestion, lineage and operational metadata is added:

| Bronze metadata column | Description |
|---|---|
| `_source_file` | Full ADLS path of the source file |
| `_file_name` | Name of the source CSV file |
| `_file_group` | Group identifier extracted from `data_group_<n>.csv` |
| `_ingested_at` | Timestamp at which Databricks ingested the record |
| `_ingestion_date` | Date on which the record was ingested |
| `_rescued_data` | Data rescued by Spark if malformed or unexpected fields occur |

[Back to top](#turbine-data-lakehouse-pipeline)

## Repository Layout

```text
.
├── Notebooks/
│   ├── NB1_Collibri_Bronze.ipynb
│   ├── NB2_Collibri_Silver.ipynb
│   └── NB3_Collibri_Gold.ipynb
├── Tests/
│   └── NB_Collibri_Bronze_Test.ipynb
├── jobflow.yaml
└── README.md
```

[Back to top](#turbine-data-lakehouse-pipeline)

## Notebooks

### Bronze Tests

**Notebook:** `Tests/NB_Collibri_Bronze_Test.ipynb`

This notebook is the validation gate for Bronze-layer logic. It runs before the ingestion task in the Databricks Job.

It tests:

- File-group extraction from source file names such as `data_group_1.csv`
- Deduplication by the business key `turbine_id` and `timestamp`
- Retention of the most recently ingested version of duplicate readings
- Delta table creation when a Bronze target table does not yet exist
- Delta `MERGE` insert behaviour for new turbine/timestamp keys
- Delta `MERGE` update behaviour for existing turbine/timestamp keys
- Basic data-quality summary metrics
- Retrieval of ordered sample data

The test notebook should use a dedicated test table, for example:

```text
interviews_dev.bronze.turbine_raw_unit_test
```

The test table must never be the production Bronze table. A failed assertion raises an error so that the Job stops before the Bronze ingestion task.

### Bronze Ingestion

**Notebook:** `Notebooks/NB1_Collibri_Bronze.ipynb`

This notebook ingests raw ADLS CSV files into the Bronze layer.

Core processing steps:

1. Reads CSV source files from the ADLS landing path
2. Infers source types and reads headers
3. Adds source-file lineage and ingestion metadata
4. Extracts `_file_group` from the input file name using a regular expression
5. Deduplicates source records using `turbine_id` and `timestamp`, retaining the latest `_ingested_at` record
6. Creates the target table if it does not exist; otherwise performs a Delta `MERGE`
7. Updates matched readings and inserts new readings
8. Calculates basic data-quality metrics: total row count, number of distinct turbines, earliest timestamp, and latest timestamp
9. Displays a sample of Bronze records and Delta table history

Target table:

```text
interviews_dev.bronze.turbine_raw
```

The `MERGE` key is:

```sql
target.turbine_id = source.turbine_id
AND target.timestamp = source.timestamp
```

This makes the Bronze load idempotent: re-running the same source file updates matching keys rather than continually inserting duplicates.

### Silver Transformation

**Notebook:** `Notebooks/NB2_Collibri_Silver.ipynb`

This notebook reads Bronze data and prepares a clean, validated Silver dataset.

Core processing steps:

1. Reads the Bronze Delta table
2. Applies data-quality checks and the expected Silver schema
3. Removes or handles records that do not meet the required quality rules
4. Produces typed, standardized turbine telemetry for downstream analytics
5. Writes the resulting dataset to the Silver layer

The Silver layer is the trusted source for analytical calculations. Column naming follows the analytics-oriented convention used in the Gold notebook, including `turbine_id`, `timestamp`, `wind_speed`, `wind_direction`, and `power_output`.

### Gold Analytics

**Notebook:** `Notebooks/NB3_Collibri_Gold.ipynb`

This notebook generates business-facing turbine analytics from Silver data.

#### Daily Power Summaries

For each `turbine_id` and calendar date, it calculates:

- Minimum power output
- Maximum power output
- Average power output
- Number of readings

The output is suitable for dashboard charts that compare minimum, average, and maximum MW across turbines.

#### Anomaly Detection

For each turbine and calendar day, the notebook calculates a mean and sample standard deviation of `power_output` using a window partitioned by:

```text
turbine_id, reading_date
```

It then calculates:

```text
lower threshold = mean - 2 × standard deviation
upper threshold = mean + 2 × standard deviation
z-score = (power output - mean) / standard deviation
```

A reading is an anomaly when:

```text
abs(z-score) > 2
```

Anomalies are labelled as:

- `HIGH_POWER_OUTPUT` when `z_score > 2`
- `LOW_POWER_OUTPUT` when `z_score < -2`

This identifies individual readings that substantially deviate from the turbine's own normal output range for that day. Turbines can legitimately have zero anomalies under this rule.

#### Gold Outputs

The Gold notebook writes or prepares tables and DataFrames such as:

```text
interviews_dev.gold.turbine_daily_summary
interviews_dev.gold.turbine_anomalies
```

It also supports visualisations including:

- A line chart of average power output over time, grouped by turbine
- A grouped bar chart of daily minimum, average, and maximum output by turbine
- A per-turbine anomaly summary including total, low-power, and high-power anomaly counts

[Back to top](#turbine-data-lakehouse-pipeline)

## Job Orchestration

`jobflow.yaml` defines a sequential Databricks Job:

```text
Bronze_Layer_TEST
  → Bronze_Layer_NB
  → Silver_Layer_NB
  → Gold_Layer_NB
```

| Task | Purpose |
|---|---|
| `Bronze_Layer_TEST` | Runs data-engineering unit tests before production ingestion |
| `Bronze_Layer_NB` | Ingests and merges ADLS CSV data into Bronze |
| `Silver_Layer_NB` | Cleans and standardizes Bronze data for analytics |
| `Gold_Layer_NB` | Produces daily summaries, anomaly scores, and Gold outputs |

The configuration includes:

- Task dependencies, preventing downstream execution after a failed upstream task
- A 600-second timeout per task
- A duration health rule that warns after 300 seconds
- Failure and duration-warning email notifications
- Queueing enabled for concurrent run handling
- A performance-optimized serverless execution target when supported by the workspace

[Back to top](#turbine-data-lakehouse-pipeline)

## Running the Project

1. Upload or sync the notebooks to the workspace paths referenced in `jobflow.yaml`
2. Ensure the job identity has permission to read the ADLS external location or storage credential used by the `abfss://` path
3. Ensure the job identity has permissions to create, read, update, and delete test tables in `interviews_dev.bronze` and to write Bronze, Silver, and Gold tables
4. Run the test notebook independently and confirm all tests pass
5. Trigger the Databricks Job
6. Inspect Job run logs, Delta table history, and Gold outputs

[Back to top](#turbine-data-lakehouse-pipeline)

## Design Considerations

- **Idempotency:** Bronze uses a Delta `MERGE` keyed on turbine and timestamp, enabling safe reruns
- **Lineage:** Bronze retains original source-file and ingestion metadata
- **Layer separation:** Raw operational data, validated data, and analytical outputs are separated into Bronze, Silver, and Gold
- **Testing isolation:** Unit tests use in-memory fixtures and a dedicated test Delta table; they should not run the production Bronze notebook or modify `turbine_raw`
- **Anomaly interpretation:** The two-standard-deviation rule is a statistical operational indicator. Production turbine-performance monitoring can be enhanced by comparing output against an expected power curve based on wind speed and turbine characteristics

[Back to top](#turbine-data-lakehouse-pipeline)

## Future Improvements

- Move shared Bronze functions into a separate function-only Python module or notebook, used by both the Bronze job notebook and tests
- Run tests in an isolated test catalog/schema rather than the development Bronze schema
- Parameterize catalog, schema, storage path, and file pattern using Databricks widgets or Job parameters
- Add data-quality expectations for nulls, duplicate rates, valid turbine IDs, and permissible measurement ranges
- Add incremental ingestion controls based on file metadata or Auto Loader checkpoints
- Store anomaly results with run metadata and create a Databricks SQL dashboard for turbine health monitoring

[Back to top](#turbine-data-lakehouse-pipeline)

## Key Notes*

- **Turbine 5:** No anomalous readings were detected under the implemented daily turbine-level rule. A reading is flagged only when its absolute z-score exceeds 2, meaning it falls outside the daily mean plus or minus two sample standard deviations.

- **ADLS file ingestion:** The source CSV files were manually uploaded to the ADLS Gen2 landing zone for this implementation. In a production design, Azure Data Factory could automate file ingestion and orchestration. Azure Function Apps could also be used to react to events such as new-file arrivals and trigger downstream processing.

[Back to top](#turbine-data-lakehouse-pipeline)