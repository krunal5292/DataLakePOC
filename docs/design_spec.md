# Design Specification: Consent-Aware Athlete Data Lakehouse

## 1. Architecture Overview
A high-performance data platform designed to handle high-frequency athlete telemetry with millisecond-latency buyer access controlled by a multi-dimensional consent matrix.

- **Storage:** Apache Iceberg (V2) on MinIO (S3-compatible).
- **Compute/Serving:** ClickHouse (OLAP) with Iceberg integration.
- **Stream Processing:** Apache Flink (Real-time enrichment).
- **State/Cache:** Redis (Bitmask storage & Kill-switch).
- **Backend:** Java 21 + Spring Boot 3.4.

---

## 2. Infrastructure Setup (Docker Compose)
The environment must include:
- **MinIO:** S3-compatible storage for the Iceberg Catalog.
- **Spark Iceberg:** Configured with `RestCatalog` for table management.
- **ClickHouse:** Must include S3 and Iceberg table engines in configuration.
- **Redis:** Used for low-latency consent bitmask lookups.

---

## 3. The Medallion Data Model

### Layer 1: Bronze (Raw)
- **Source:** Wearable/IoT Ingestion.
- **Format:** Iceberg.
- **Schema:** `(id UUID, athlete_id STRING, raw_json STRING, ingested_at TIMESTAMP)`.
- **Partition:** `days(ingested_at)`.

### Layer 2: Silver (Structured)
- **Source:** Spark/Flink job cleaning Bronze data.
- **Format:** Iceberg.
- **Schema:** - `athlete_id` (String)
    - `activity_id` (String)
    - `heart_rate` (Double)
    - `blood_pressure_sys` (Double)
    - `oxygen_sat` (Double)
    - `ts` (Timestamp)
    - `consent_version` (Integer)
- **Partition:** `identity(athlete_id)`.

### Layer 3: Gold (Serving)
- **Technology:** ClickHouse MergeTree Engine.
- **Added Column:** `consent_mask` (UInt64).
- **Logic:** Data is synced from Silver; every row is tagged with the **active bitmask** for that athlete at that timestamp.

---

## 4. Consent Bitmask Logic
To avoid N*N joins, dimensions are mapped to a 64-bit integer.

### Static Bit Mapping:
- **Bit 0:** Purpose: RESEARCH
- **Bit 1:** Purpose: COMMERCIAL
- **Bit 5:** Activity: RUNNING (ID: 1)
- **Bit 6:** Activity: CYCLING (ID: 2)
- **Bit 10:** Field: HEART_RATE
- **Bit 11:** Field: BLOOD_PRESSURE

### Example Calculation:
If an athlete allows **Heart Rate** for **Research** in **Activity 1**:
`Mask = (2^0) | (2^5) | (2^10) = 1057`.

---

## 5. Implementation Requirements

### A. Java Spring Boot (Consent Service)
- **Entity:** `ConsentRule` mirroring the provided JSON structure.
- **Service:** `BitmaskGenerator` to convert JSON logic into a `Long`.
- **Integration:** Update Redis on every Rule change; trigger ClickHouse `ALTER TABLE UPDATE` for existing rows.

### B. Buyer API (Fetching)
- **Query Builder:** Must calculate the "Required Mask" based on the Buyer's persona and requested fields.
- **Filtering:** Use Bitwise AND in SQL: `WHERE (consent_mask & REQUIRED_MASK) == REQUIRED_MASK`.

---

## 6. Operation Scenarios
1. **New Rule:** Calculate mask -> Update Redis -> Patch ClickHouse.
2. **Revocation:** Set `Status: INACTIVE` -> Flush Redis key -> API returns 403/Empty.
3. **Data Fetch:** Millisecond retrieval from ClickHouse using indexed bitwise filters.

---

## 7. POC Data Flow Implementation Strategy
To streamline the POC without requiring a full Spark/Flink cluster, the data pipeline will be orchestrated within the Spring Boot application using Scheduled Tasks.

### A. Ingestion (Bronze Layer)
- **Endpoint:** `POST /api/ingest`
- **Payload:** Raw JSON telemetry (e.g., `{ "athlete_id": "...", "data": { ... } }`)
- **Action:**
  1.  Generate UUID and Timestamp.
  2.  Write raw JSON file to MinIO bucket `bronze`.
  3.  Path: `bronze/YYYY/MM/DD/{uuid}.json`.

### B. Processing (Silver Layer)
- **Trigger:** `@Scheduled` task (every 30 seconds).
- **Action:**
  1.  List new files in MinIO `bronze`.
  2.  Parse JSON and validate schema.
  3.  Flatten structure (extract `heart_rate`, `bp`, etc.).
  4.  Write structured data to MinIO `silver` (as CSV/Parquet for ClickHouse consumption).
  5.  Path: `silver/athlete_id={id}/{timestamp}.csv`.

### C. Serving Sync (Gold Layer)
- **Trigger:** `@Scheduled` task (every 1 minute) or Event-based.
- **Action:**
  1.  Read new `silver` files.
  2.  Fetch active **Consent Mask** from Redis for the `athlete_id`.
  3.  Insert row into ClickHouse `athlete_data_gold` with the computed `consent_mask`.