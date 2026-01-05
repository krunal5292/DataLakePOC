# Implementation Plan: Iceberg Physical Partitioning (V2)
**Date:** January 05, 2026
**Status:** 🚧 PROPOSED
**Reference Spec:** [`design_spec_v2_deep_dive_physical_partitioning.md`](file:///Users/krunal/REPO/DataLakePOC/docs/design_spec_v2_deep_dive_physical_partitioning.md)

## 1. Goal
Implement the **Iceberg-based Physical Partitioning Strategy** for the Gold Layer.
This upgrades the physical storage from simple MinIO objects to managed Apache Iceberg tables, enabling:
- Hierarchical Partitioning (`purpose` -> `activity` -> `event` -> `day`) for efficient pruning.
- Metadata-based "Virtual" Partition Evolution (e.g., adding `sport` later without rewriting files).
- Robust Access Control via physical separation.

## 2. Architecture Changes
- **New Components**:
    - **Iceberg REST Catalog**: To manage table metadata.
    - **PostgreSQL**: Backend for the REST Catalog.
- **Data Flow**:
    - `SilverEnrichment` -> `IcebergPhysicalPartitionStrategy` -> `Iceberg REST Catalog` -> `MinIO (Parquet Files)`
- **Co-existence Strategy**:
    - **Legacy**: `PhysicalPartitionStrategy` (Active by default or when `consent.enforcement.strategy=partition`).
    - **New**: `IcebergPhysicalPartitionStrategy` (Active when `consent.enforcement.strategy=iceberg`).
    - **Tests**: Existing tests will continue to use `partition` strategy and PASS without modification. New tests will explicitly enable `iceberg`.

## User Review Required

> [!IMPORTANT]
> **New Infrastructure**: This plan adds 2 new containers (`iceberg-rest`, `postgres`) to your local Docker environment.

> [!NOTE]
> **Data Marts & Evolution**:
> - **Partition Evolution**: Supported natively by Iceberg (metadata operations). We will demonstrate this in the verification phase.
> - **Data Marts**: This implementation builds the **Main Gold Table**. Data Marts are separate Iceberg tables that can be derived from this Main Table. We will establish the pattern here.

## 3. Implementation Steps

### A. Infrastructure & Dependencies

#### 1. [MODIFY] [`pom.xml`](file:///Users/krunal/REPO/DataLakePOC/pom.xml)
- Add Apache Iceberg Dependencies:
    - `org.apache.iceberg:iceberg-core`
    - `org.apache.iceberg:iceberg-parquet`
    - `org.apache.iceberg:iceberg-aws` (S3 integration)
    - `org.postgresql:postgresql` (Runtime dep for JDBC if needed, though mostly for the container)

#### 2. [MODIFY] [`docker-compose.yml`](file:///Users/krunal/REPO/DataLakePOC/docker-compose.yml)
- Add `iceberg-rest` service (Image: `tabulario/iceberg-rest`)
- Add `db-catalog` service (Image: `postgres:15-alpine`)
- Configure MinIO environment variables for Iceberg compatibility if needed (region, etc).

#### 3. [MODIFY] [`application.properties`](file:///Users/krunal/REPO/DataLakePOC/src/main/resources/application.properties)
- Add Iceberg Catalog configuration:
    ```properties
    iceberg.catalog.uri=http://localhost:8181
    iceberg.catalog.warehouse=s3://gold-warehouse/
    iceberg.s3.endpoint=http://localhost:9000
    iceberg.s3.access-key=minio
    iceberg.s3.secret-key=minio123
    ```

### B. Core Strategy Implementation

#### [NEW] [`IcebergPhysicalPartitionStrategy.java`](file:///Users/krunal/REPO/DataLakePOC/src/main/java/org/example/processing/strategy/IcebergPhysicalPartitionStrategy.java)
- **Implements**: `GoldEnforcementStrategy`
- **Condition**: `@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "iceberg")`
- **Responsibilities**:
    1.  **Initialize**: Check if `gold_telemetry` table exists in Catalog. If not, create it with Partition Spec:
        - `identity(purpose)`
        - `identity(activity_type)`
        - `identity(event_id)`
        - `day(timestamp)`
    2.  **Process (Fan-Out)**:
        - Convert incoming `TelemetryMessage` + Enriched Data into `Iceberg Record`.
        - Write to the table. Iceberg handles the physical folder structure.
    3.  **Revocation**:
        - Use Iceberg `DeleteFile` or `RowDelta` to delete records matching `athlete_id` AND `purpose`.
    4.  **Verify**:
        - Query the table via Iceberg API to confirm data matches expected partition.

### C. Test Implementation

#### [NEW] [`IcebergStrategyIntegrationTest.java`](file:///Users/krunal/REPO/DataLakePOC/src/test/java/org/example/integration/IcebergStrategyIntegrationTest.java)
- **Type**: Testcontainers Integration Test
- **Setup**:
    - Spins up MinIO, Postgres, Iceberg REST.
- **Tests**:
    1.  `testFanOutCreatesCorrectPartitions()`: Write data -> Inspect MinIO object paths to ensure they match `purpose=X/activity=Y/...` structure.
    2.  `testRevocationRemovesData()`: Write data -> Call revoke -> Assert Iceberg scan returns 0 records.
    3.  `testSchemaEvolution()`: (Optional) Add a column -> Write new data -> Verify read works for both.
    4.  **`testDataMartCreation()`**:
        - Create a new table `gold_megacorp` with a different partition spec (`identity(sport)`).
        - Read data from `gold_telemetry` (Main Table).
        - Write into `gold_megacorp`.
        - **Verify**: Data exists in `gold_megacorp` with its own physical layout, isolated from Main Gold.

## 4. Testing Strategy (Strict End-to-End)

We will use a **Multi-Layered Testing Approach** to ensure 100% correctness of the physical partitioning.

### A. Integration Testing (Automated via Testcontainers)
**File**: `src/test/java/org/example/integration/IcebergStrategyIntegrationTest.java`
**Stack**: `MinIO` + `Postgres` + `Iceberg REST Catalog`

| Test Scenario | Input | Expected Outcome | Verification Method |
| :--- | :--- | :--- | :--- |
| **1. Primary Fan-Out** | `TelemetryMessage` (Running, HR=140) with Consent `[Research, Marketing]` | **Two** logical records in Iceberg table. **Physical isolation** in MinIO. | 1. Query Iceberg: `SELECT * FROM gold WHERE purpose='research'` returns 1 row.<br>2. **Physical Check**: `minioClient.listObjects` confirms path contains `/purpose=research/` and `/purpose=marketing/`. |
| **2. Access Control** | Query `Gold` table for `purpose=education` (Not granted) | **Zero** records. | Iceberg Query returns empty result set. |
| **3. Revocation** | Call `handleRevocation('athlete1', 'research')` | Data physically removed or marked deleted. | 1. Query `purpose=research` -> Empty.<br>2. Query `purpose=marketing` -> **Unchanged** (Data must still exist). |
| **4. Partition Evolution** | 1. Write Data (Old Spec)<br>2. `ALTER TABLE ADD COLUMN sport`<br>3. `ALTER SPEC ADD identity(sport)`<br>4. Write Data (New Spec) | Table contains mixed data. Queries work across both. | Query `SELECT * FROM gold WHERE sport='football'` returns new data only. Query without filter returns ALL. |
| **5. Data Mart Isolation** | 1. Create `gold_megacorp`<br>2. Insert into `gold_megacorp` from `gold_telemetry` | `gold_megacorp` has its own metadata and files. | Verify operations on `gold_megacorp` do NOT affect `gold_telemetry`. Verify `gold_megacorp` partition structure differs. |

### B. Manual End-to-End Verification
**Prerequisite**: `docker-compose up -d` (including new Iceberg services).

1.  **Ingest Verification**:
    - Send curl request to Ingestion API (or produce to Kafka).
    - Watch logs: `IcebergPhysicalPartitionStrategy: Commited snapshot 12345...`.
2.  **Visual Inspection (MinIO Console)**:
    - Go to `http://localhost:9001`.
    - Browse `gold-warehouse` bucket.
    - **Pass Criteria**: You must see the deep nested folder structure: `data/purpose=.../activity=.../event=.../day=.../`.
3.  **Catalog Verification**:
    - We will provide a simple generic `IcebergClient` utility to list tables and snapshots from the command line.

## 5. Execution Checklist
- [ ] Update `pom.xml` and `docker-compose.yml` <!-- id: 5 -->
- [ ] Create `IcebergPhysicalPartitionStrategy.java` <!-- id: 6 -->
- [ ] Create `IcebergStrategyIntegrationTest.java` <!-- id: 7 -->
- [ ] Run Tests & Verify <!-- id: 8 -->
