# Implementation Plan: Kafka-Driven Hybrid Architecture (Rev 18)
**Date:** December 31, 2025

## 1. Overview
This plan integrates **Apache Kafka** to decouple ingestion from storage, enabling high-throughput, non-blocking telemetry acquisition. The architecture retains the **Hybrid Gold Layer** (Consent-Partitioned Storage) and uses Kafka to orchestrate data movement from the API to the final compliant storage.

---

## 2. Infrastructure Stack (Docker Compose)
- **MinIO:** Object Storage (Bronze, Silver, Gold).
- **Redis:** Fast Consent Rule lookups.
- **Zookeeper & Kafka:** Event streaming platform.
- **Kafka-UI:** Web interface (Port 8080) to monitor topics, messages, and consumer groups.
- **ClickHouse:** (Optional) Long-term OLAP storage.

---

## 3. Data Flow Architecture & Diagram

```mermaid
graph TD
    API[REST API<br/>(IngestionController)] -->|Publish Raw Payload| T1[Topic: telemetry-ingest]
    
    subgraph "Bronze Layer"
        T1 -->|Consume| L1[Listener: BronzeConsumer]
        L1 -->|Write Raw JSON| S1[(MinIO Bronze)]
        L1 -->|Publish File Path| T2[Topic: telemetry-bronze-saved]
    end

    subgraph "Silver Layer"
        T2 -->|Consume| L2[Listener: SilverEnrichmentProcessor]
        L2 -->|Read Raw File| S1
        L2 -->|Enrich (Names/Events)| L2
        L2 -->|Write Enriched JSON| S2[(MinIO Silver)]
        L2 -->|Publish File Path| T3[Topic: telemetry-silver-saved]
    end

    subgraph "Gold Layer (Consent Engine)"
        T3 -->|Consume| L3[Listener: GoldConsentProcessor]
        L3 -->|Read Enriched File| S2
        L3 -->|Fetch Rules| DB[(Redis Consent Cache)]
        
        L3 -->|Filter & Fan-Out| G1[(MinIO Gold<br/>purpose=RESEARCH)]
        L3 -->|Filter & Fan-Out| G2[(MinIO Gold<br/>purpose=MEDICAL)]
        L3 -->|Filter & Fan-Out| G3[(MinIO Gold<br/>purpose=COMMERCIAL)]
    end
```

---

## 4. End-to-End Event Flow
*(Claim Check Pattern: Kafka moves notifications, MinIO holds data)*
1. **Ingestion:** API receives JSON -> Generates `trace_id` (UUID) -> Publishes to `telemetry-ingest`.
2. **Bronze:** `BronzeConsumer` writes raw JSON to `bronze/pending/YYYY-MM-DD/{athlete_id}/{trace_id}.json` -> Publishes `telemetry-bronze-saved`.
3. **Silver:** `SilverEnrichmentProcessor` adds Master Data -> Writes to `silver/pending/YYYY-MM-DD/{athlete_id}/{trace_id}.json` -> Publishes `telemetry-silver-saved`.
4. **Gold:** `GoldConsentProcessor` applies complex rules -> Fan-out write to `gold/YYYY-MM-DD/purpose=.../athlete_id=.../data_{trace_id}.json`.

---

## 5. Resiliency & Failure Handling
- **Idempotency:** Strict use of `trace_id` (UUID) for all filenames. Overwrites prevent duplicates on retry.
- **Retries:** 3-attempt exponential backoff + Dead Letter Queues (DLQ) for all consumers.
- **POC Strategy:** Fail-fast on Ingestion API if Kafka is unreachable.

---

## 6. Implementation Steps
- **Phase 1: Infrastructure.** Update `docker-compose.yml` (Kafka, Zookeeper, Kafka-UI) and `pom.xml`.
- **Phase 2: Ingestion.** Implement Producer logic and Trace ID generation.
- **Phase 3: Processing.** Implement Bronze, Silver, and Gold Kafka Listeners.
- **Phase 4: Testing.** Create `DataFlowKafkaTestcontainersTest` and `DataFlowKafkaManualTest`.

---

## 7. Data Examples

### A. Raw Data Example (13 Rows)
```csv
id,athlete_id,event_id,sensor_type,field,value,value1,value2,value3,timestamp
d1,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,VO2_MAX,52.5,0,0,0,2025-11-17T15:32:13Z[UTC]
d2,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,SLEEP_QUALITY,85,0,0,0,2025-11-17T15:32:13Z[UTC]
d3,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,RECOVERY_CAPACITY,78,0,0,0,2025-11-17T15:32:13Z[UTC]
d4,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,PHYSIOLOGICAL_READINESS,92,0,0,0,2025-11-17T15:32:13Z[UTC]
d5,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,NEUROLOGICAL_RECOVERY,88,0,0,0,2025-11-17T15:32:13Z[UTC]
d6,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,HEART_RATE_VARIABILITY,65,0,0,0,2025-11-17T15:32:13Z[UTC]
d7,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,HEART_RATE,58,0,0,0,2025-11-17T15:32:13Z[UTC]
d8,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,ENDURANCE,82,0,0,0,2025-11-17T15:32:13Z[UTC]
d9,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,CARDIAC_LONGEVITY,95,0,0,0,2025-11-17T15:32:13Z[UTC]
d10,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,BODY_COMPOSITION,12.5,0,0,0,2025-11-17T15:32:13Z[UTC]
d11,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,BLOOD_PRESSURE,115,75,0,0,2025-11-17T15:32:13Z[UTC]
d12,f1db1530-d081-708c-cb55-917d0a01f06b,summer_cup_2025,REST,BIOMECHANICS,88,0,0,0,2025-11-17T15:32:13Z[UTC]
d13,f1db1530-d081-708c-cb55-917d0a01f06b,wimbledon_2025,MATCH,HEART_RATE,175,0,0,0,2025-06-25T10:00:00Z[UTC]
```

### B. Full Consent Rule Example (Exact)
```json
{
  "_id": { "$oid": "695516b00140e373254469c0" },
  "ruleId": "RULE0000365",
  "ruleName": "Krunal-v1",
  "description": "I consent to share my data that has the combination...",
  "userId": "f1db1530-d081-708c-cb55-917d0a01f06b",
  "consentCreator": "ffff llllll",
  "consentCreatorPersona": "PLAYER",
  "status": "ACTIVE",
  "dimensions": {
    "purpose": {
      "type": "specific",
      "values": [
        { "id":"1","value":"research","title":"Research" },
        { "id":"11","value":"governmentResearch","title":"Government Research" },
        { "id":"12","value":"privateResearch","title":"Private Research" },
        { "id":"2","value":"sportsAndPerformance","title":"Sports & Performance" },
        { "id":"21","value":"playerPerformanceEvaluation","title":"Player Performance Evaluation" },
        { "id":"22","value":"trainingOptimisation","title":"Training Optimisation" },
        { "id":"3","value":"healthAndMedical","title":"Health & Medical" },
        { "id":"31","value":"clinicalAnalysis","title":"Clinical Analysis" },
        { "id":"32","value":"physiologicalMonitoring","title":"Physiological Monitoring" },
        { "id":"4","value":"education","title":"Education" },
        { "id":"41","value":"universityResearch","title":"University Research" },
        { "id":"42","value":"academicStudies","title":"Academic Studies" },
        { "id":"5","value":"technologyAndAI","title":"Technology & AI" },
        { "id":"51","value":"aiModelTraining","title":"AI Model Training" },
        { "id":"52","value":"machineLearningResearch","title":"Machine Learning Research" }
      ]
    },
    "activity": { "type": "any", "values": [] },
    "events": {
      "type": "specific",
      "values": [
        { "id":"1","value":"summer_cup_2025","title":"Summer Cup 2025" },
        { "id":"2","value":"wimbledon_2025","title":"Wimbledon 2025" }
      ]
    },
    "consumerType": { "type": "any", "values": [] },
    "consumerId":   { "type": "any", "values": [] },
    "fields":       { "type": "any", "specifications": [] },
    "temporal":     { "type": "any" },
    "price":        { "type": "any" },
    "validity":     { "type": "any" },
    "anonymization": {
      "type": "any",
      "required": false,
      "values": [
        { "id":"0","value":"noAnonymous","title":"No Anonymous" }
      ]
    }
  },
  "conflicts": { "hasConflicts": false,"conflictCount": 0,"conflictingRules": [] },
  "createdBy": "f1db1530-d081-708c-cb55-917d0a01f06b",
  "createdAt": { "$date": "2025-12-31T12:27:28.670Z" },
  "updatedBy": "f1db1530-d081-708c-cb55-917d0a01f06b",
  "updatedAt": { "$date": "2025-12-31T12:27:28.670Z" },
  "deleted": false,
  "_class": "com.sportsdatalabs.consent.entity.ConsentRule"
}
```

---

## 8. Simulation: Indexing Result

**Input:** Raw Rows d1-d13 + Consent Rule Krunal-v1.

**Resulting Storage Paths in Gold Layer:**

#### `/gold/2025-12-31/purpose=research/athlete_id=f1db.../`
- `data_trace1.json` (Contains full telemetry: VO2, Heart Rate, BP, etc.)
- **Success Criteria:** 'research' is in the allow-list. All 13 rows match event criteria.

#### `/gold/2025-12-31/purpose=marketing/athlete_id=f1db.../`
- **Result:** **No data generated**.
- **Reason:** 'marketing' is NOT in the allowed list of purposes.

---

## 9. Testing Strategy: End-to-End Compliance Verification

The goal is to verify the entire pipeline—from raw ingestion to partitioned gold storage—using the exact data and consent provided above.

### A. Automated E2E Test (Testcontainers)
- **Class:** `DataFlowKafkaEndToEndTest`
- **Setup:**
  1. Spin up Kafka, Redis, MinIO via Testcontainers.
  2. **Seed Consent:** Insert the "Krunal-v1" Consent Rule into Redis for athlete `f1db1530...`.
- **Execution:**
  1. **Hit System:** Perform `POST /api/ingest` with the 13 raw data rows.
  2. **Await Processing:** Use Awaitility to wait for Kafka consumers to finish (Bronze -> Silver -> Gold).
- **Verification:**
  1. **Bronze Check:** Assert file exists at `bronze/pending/.../{trace_id}.json`.
  2. **Silver Check:** Assert enriched file exists at `silver/pending/.../{trace_id}.json` (verify Athlete Name injected).
  3. **Gold Indexing Check:**
     - Assert file exists in `gold/2025-12-31/purpose=research/...`.
     - Assert file DOES NOT exist in `gold/2025-12-31/purpose=marketing/...`.
     - Verify file content matches the filtered projection of the raw data.

### B. Manual E2E Test (Persistent Infrastructure)
- **Class:** `DataFlowKafkaManualTest`
- **Scenario:** Runs against the persistent `docker-compose` stack.
- **Verification:** 
  - Open **Kafka-UI** (Port 8080) to view messages in `telemetry-silver-saved`.
  - Open **MinIO Browser** (Port 9001) to visually confirm the partitioned folder structure in the `gold` bucket.