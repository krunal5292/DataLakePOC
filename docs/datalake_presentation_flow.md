# Data Lake Architecture: Integrated Tool & Flow View

This document provides a **presentation-ready visualization** of our Data Lake. It maps every step of the data flow to the specific **Technology** used and defines the exact **Job** that tool performs.

## 1. The "Tool-to-Job" Map

Use this diagram to present the end-to-end flow to your team.

### ASCII Flow Design (Universal)

```text
[Device] --> (Ingest API) --+--> [Redis Cache]
                            |
                            v
                    (Kafka Event Bus)
                            |
           +----------------+----------------+
           |                |                |
           v                v                v
    [Bronze Consumer]  [Silver Proc.]   [Gold Processor]
           |                |                |
           v                v                v
    [(MinIO: Raw)]   [(MinIO: Clean)]  [(Iceberg: Gold)]
                                             |
                                             v
                                     [Postgres Catalog]
```

### Generated Image
I have also generated a high-quality image file `data_lake_architecture_diagram.png` for you to use in slides. You can find it in the artifacts folder.

---

## 2. Detailed "Who Does What" Summary

| # | Layer / Step | Technology | The Job (Purpose) |
| :--- | :--- | :--- | :--- |
| **1** | **Ingestion** | **Spring Boot** | Acts as the **Gatekeeper**. It accepts data via REST API, ensures it's valid JSON, and checks if the user allowed data collection. |
| **2** | **Consent Check** | **Redis** | Acts as the **Speed Trap**. It holds an extremely fast, in-memory copy of consent rules so the API doesn't have to query a slow database for permission. |
| **3** | **Transport** | **Apache Kafka** | Acts as the **Conveyor Belt**. It decouples the fast API from the slower processing layers. If the "factory" (processing) gets backed up, the "conveyor belt" just holds the items safely. |
| **4** | **Bronze Layer** | **MinIO (S3)** | Acts as the **Raw Archive**. Stores data *exactly* as it arrived (JSON). We never update/delete this; it's our "source of truth" if we need to replay history. |
| **5** | **Silver Layer** | **Spring Boot + MinIO** | Acts as the **Refinery**. The processor fixes bad formatting, converts units (e.g., F to C), and saves a "clean" version (JSON) to MinIO. |
| **6** | **Gold Layer** | **Apache Iceberg** | Acts as the **Warehouse**. Data is converted to **Parquet** (super fast for analytics), partitioned by useful columns (like `activity` or `date`), and organized into tables using the Iceberg Hub. |
| **7** | **Governance** | **Iceberg Catalog (Postgres)** | Acts as the **Librarian**. It knows exactly which file belongs to which version of the table. It ensures that if two people write at the same time, nothing gets corrupted (ACID). |

---

## 3. Why this separation?

*   **Reliability**: If the **Gold Processor** crashes, the **Bronze Data** is already safe in MinIO. We can just re-run the processing later.
*   **Scalability**: We can run 10 copies of the **Silver Processor** to handle a traffic spike without touching the Bronze or Gold layers.
*   **Flexibility**: **MinIO** lets us develop locally as if we were on AWS Cloud. **Testcontainers** lets us test this entire complex diagram on a single laptop.

---

## 4. Detailed Data Transaction Flow

This diagram zooms in to show the **specific events and data transformations** happening at each step.

```text
[CLIENT DEVICE]
      |
      | (1) POST JSON { "heart_rate": 80, "athlete_id": "123" }
      v
+------------------+
|    INGEST API    |
| (Spring Boot)    |
+------------------+
      |
      +---(2) Check Consent? ---> [REDIS] (Returns: ALLOWED)
      |
      | (3) Publish Event: "telemetry.ingest"
      v
[KAFKA: ingest-topic]
      |
      +-------------------------+
                                |
                                v
                      +-------------------+
                      |  BRONZE CONSUMER  |
                      |   (Raw Saver)     |
                      +-------------------+
                                | (4) Write File: "bronze/123/trace.json"
                                v
                         [(MinIO: BRONZE)] <--- Immutable Source of Truth
                                |
                                | (5) Publish Event: "bronze.saved"
                                v
                      [KAFKA: bronze-saved]
                                |
                                v
                      +-------------------+
                      | SILVER PROCESSOR  |
                      | (Enrich/Validate) | <--- Converts Units (F -> C)
                      +-------------------+
                                | (6) Write File: "silver/123/enriched.json"
                                v
                         [(MinIO: SILVER)]
                                |
                                | (7) Publish Event: "silver.saved"
                                v
                       [KAFKA: silver-saved]
                                |
                                v
                      +-------------------+
                      |   GOLD PROCESSOR  |
                      | (Fan-out Logic)   | <--- Re-checks Consent (Strict)
                      +-------------------+
                                |
        +-----------------------+-----------------------+
        | (8a) If Purpose="Research"                    | (8b) If Purpose="Marketing"
        v                                               v
[(MinIO: Gold - Parquet)]                       (DROP / IGNORE)
(Path: purpose=research/...)
        |
        +---(9) COMMIT SNAPSHOT ---> [ICEBERG REST CATALOG]
                                              |
                                              v
                                      [(Postgres DB)]
```
