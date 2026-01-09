# Integration Test Suite Summary

This document provides a detailed breakdown of the integration test suite for the Data Lake POC. It focuses on validating the end-to-end data flow, consent enforcement, and storage strategies (specifically Iceberg) using Testcontainers.

## 1. IcebergStrategyIntegrationTest.java

**Focus:** Gold Layer (Storage & Partitioning)
**Context:** Runs in a **completely isolated environment** (Dedicated Kafka, Redis, MinIO, Postgres) to prevent consumer group conflicts.

### Test Cases

#### `testFullIcebergFanOutFlow`
*   **Goal:** Verify the "Fan-Out" logic where data is routed to specific Iceberg tables/partitions based on consent rules.
*   **Workflow:**
    1.  Loads `consent_rule.json` (Consent Rule: "active" for "research") and `raw_data.json`.
    2.  Ingests data via the REST API (`/api/ingest`).
    3.  Waits for the asynchronous pipeline to process data and write to the Iceberg table (`gold.telemetry`).
*   **Validation:**
    *   **Positive Consent:** Verifies data **exists** for the "research" purpose.
    *   **Negative Consent:** Verifies data **does not exist** for the "marketing" purpose (which was not consented).
    *   **Partitioning:** Validates the physical partition structure: `purpose` / `activity_type` / `event_id` / `day(timestamp)`.
    *   **Schema integrity:** ensures written records match the input.

#### `testRevocationRemovesData`
*   **Goal:** Verify that revoking consent triggers the "Right to be Forgotten" (deletion of data).
*   **Workflow:**
    1.  Ingests data for a user with ACTIVE consent for "research".
    2.  Confirms data is visible in the Gold layer.
    3.  **Triggers Revocation:** Calls `strategy.handleRevocation(athleteId, "research")`.
*   **Validation:**
    *   **Pre-Revocation:** `verifyDataExists("research")` returns `true`.
    *   **Post-Revocation:** `verifyDataExists("research")` returns `false`.

#### `testDataMartCreation`
*   **Goal:** Verify the ability to create separate "Data Marts" (tables with different schemas/partitions) for specific consumers.
*   **Workflow:**
    1.  Creates a new Iceberg table `gold.megacorp` with a simplified partition spec (just `activity`).
    2.  Manually writes a Parquet file to this table.
*   **Validation:**
    *   **Isolation:** The new table exists independently of the main telemetry table.
    *   **Querying:** Data can be read back using the new schema.

#### `testPartitionEvolution`
*   **Goal:** Verify Iceberg's "Partition Evolution" capabilities (changing physical layout without rewriting old data).
*   **Workflow:**
    1.  Creates a table partitioned by `purpose`, `activity`, etc.
    2.  Writes data.
    3.  **Updates Partition Spec:** Adds `trace_id` as a new partition field.
    4.  Writes new data.
*   **Validation:**
    *   **Schema Evolution:** The table metadata now reflects 5 partition fields.
    *   **Read Compatibility:** Both old and new data can be queried seamlessly.

#### `testCustomHierarchy`
*   **Goal:** Verify complex, hierarchical partition paths for data governance/organization.
*   **Workflow:** Defines a table partitioned by `region` -> `department`.
*   **Validation:**
    *   **Physical Path:** Checks that files are stored in `s3://.../region=EMEA/department=Sales/...`, validating the folder structure.

#### `testReindexing`
*   **Goal:** Verify the ability to rewrite existing data into a new partition layout.
*   **Workflow:**
    1.  Writes data partitioned only by `department`.
    2.  Updates spec to partition by `department` AND `region`.
    3.  Calls `strategy.reindexData()`.
*   **Validation:**
    *   **Physical Movement:** confirms that the original data files have been rewritten into the new folder structure.

---

## 2. DataFlowKafkaTestcontainersTest.java

**Focus:** Streaming Pipeline (Bronze -> Silver -> Gold)
**Context:** Shared container ecosystem (simulating the main application environment).

#### `testFullKafkaFlow_Testcontainers`
*   **Goal:** Validate the movement of data through all Kafka topics and MinIO buckets.
*   **Workflow:**
    1.  Loads comprehensive consent rules.
    2.  Ingests telemetry data.
    3.  Waits for the data to traverse `ingest` -> `bronze` -> `silver` -> `gold`.
*   **Validation:**
    *   **Bronze Layer:** Raw JSON exists in the `bronze` MinIO bucket.
    *   **Silver Layer:** Enriched/Sanitized data exists in the `silver` MinIO bucket.
    *   **Gold Layer (Dynamic):**
        *   Iterates through **ALL** purposes defined in the consent rule (e.g., "research", "health_analytics") and verifies data presence.
        *   Explicitly verifies that **NO data** exists for unconsented purposes (e.g., "marketing").

---

## 3. DataFlowReadPathTestcontainersTest.java

**Focus:** Query Path & Enforcement (API Consumer Perspective)
**Context:** Shared container ecosystem.

#### `testEndToEndConsentEnforcement`
*   **Goal:** Verify that the Query API (`/api/query`) enforces consent at read time.
*   **Workflow:**
    1.  Ingests data for an athlete.
    2.  Wait for processing.
    3.  **Client Queries:** Simulates a client requesting data via HTTP GET.
*   **Validation:**
    *   **Authorized Access:** Querying for a consented purpose (e.g., "research") returns `HTTP 200 OK` and the list of records.
    *   **Unauthorized Access:** Querying for an unconsented purpose (e.g., "advertising") returns an empty list (or access denied, depending on implementation).
    *   **Content verification:** Manually checks that the returned data objects actually match the requested purpose.
    *   **Test Output:** Dumps the JSON responses to `target/test-output` for manual inspection.
