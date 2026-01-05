# Implementation Plan: Consent-Aware Athlete Data Lake (v2)

**Date:** January 05, 2026  
**Status:** DRAFT  
**Version:** 2.0 (Major Redesign)

---

## Executive Summary

This implementation plan outlines the construction of a **Consent-Aware Athlete Data Lake** capable of:
- Processing telemetry data from **millions of athletes**
- Enforcing **N*N multi-dimensional consent rules** (Purpose × Activity × Event × Time × Metric × Consumer)
- Supporting **diverse ingestion sources** (API, Streaming, Socket, File, Pre-aggregated data)
- Delivering **zero-latency reads** through physical partitioning
- Enabling **dynamic re-indexing** when consent changes

**Key Architectural Shift from v1:**
- **From**: Bitmask-based filtering
- **To**: Physical Partitioning (Fan-Out) + Policy Engine

---

## Phase 1: Foundation & Infrastructure Setup

### 1.1 Technology Stack Finalization

**Core Components:**
- **Storage**: MinIO (S3-compatible object store)
- **Table Format**: Apache Iceberg (ACID, Schema Evolution, Partition Evolution)
- **Stream Processing**: Apache Flink (Real-time consent enforcement)
- **Query Engine**: Trino (OLAP queries on Iceberg tables)
- **Consent Store**: Redis (Fast rule lookups)
- **Policy Engine**: Open Policy Agent (OPA) or Custom Rule Evaluator
- **Message Bus**: Apache Kafka (Event streaming)
- **Backend**: Java 21 + Spring Boot 3.4

**Docker Compose Services:**
```yaml
services:
  - minio (S3 storage)
  - kafka + zookeeper (Event streaming)
  - redis (Consent cache)
  - flink-jobmanager + flink-taskmanager (Stream processing)
  - trino (Query engine)
  - schema-registry (Avro/JSON schema management)
```

**Deliverables:**
- [ ] `docker-compose.yml` with all services
- [ ] Network configuration (service discovery)
- [ ] Volume mounts for persistence
- [ ] Health check endpoints

---

### 1.2 MinIO Bucket Structure

**Bucket Layout:**
```text
s3://datalake/
├── bronze/          # Raw ingestion (Immutable audit trail)
├── silver/          # Cleaned, standardized data (Source of truth)
├── gold/            # Consent-enforced, partitioned serving layer
│   ├── active/      # Currently accessible data
│   └── history/     # Revoked/archived data
└── metadata/        # Iceberg catalog metadata
```

**Deliverables:**
- [ ] MinIO bucket creation scripts
- [ ] Access policies (Read/Write segregation)
- [ ] Lifecycle policies (Bronze retention: 7 years, etc.)

---

## Phase 2: Data Ingestion Layer (Bronze)

### 2.1 Multi-Channel Ingestion Service

**Supported Channels:**
1. **REST API** (`POST /api/ingest`)
2. **Kafka Producer** (Direct stream publishing)
3. **WebSocket** (Real-time telemetry)
4. **Batch File Upload** (`POST /api/ingest/batch`)

**Implementation:**
```java
@RestController
public class IngestionController {
    
    @PostMapping("/api/ingest")
    public ResponseEntity<IngestResponse> ingestTelemetry(@RequestBody TelemetryEvent event) {
        // 1. Validate schema
        // 2. Generate trace_id
        // 3. Write to Bronze (MinIO)
        // 4. Publish to Kafka (telemetry.raw)
        // 5. Return acknowledgment
    }
    
    @PostMapping("/api/ingest/batch")
    public ResponseEntity<BatchIngestResponse> ingestBatch(@RequestParam("file") MultipartFile file) {
        // Handle CSV/JSON batch uploads
    }
}
```

**Schema Support:**
- **Raw Events**: Individual data points (HR, GPS, Power)
- **Aggregated Data**: Pre-computed summaries (Whoop daily recovery scores)

**Deliverables:**
- [ ] `IngestionController` with all endpoints
- [ ] Schema validation (JSON Schema or Avro)
- [ ] Bronze writer (MinIO client)
- [ ] Kafka producer integration
- [ ] Error handling & retry logic

---

## Phase 3: Stream Processing (Silver Layer)

### 3.1 Flink Data Cleaning Job

**Responsibilities:**
1. **Consume** from `kafka://telemetry.raw`
2. **Validate** against Schema Registry
3. **Standardize**:
   - Convert Whoop JSON → Standard format
   - Normalize units (bpm, watts, etc.)
   - Extract common fields (athlete_id, timestamp, activity_type)
4. **Enrich**:
   - Add event_context (from external API or lookup table)
   - Add sport classification
5. **Write** to Silver (Iceberg table)

**Flink Job Structure:**
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<TelemetryEvent> rawStream = env
    .addSource(new FlinkKafkaConsumer<>("telemetry.raw", schema, props));

DataStream<StandardizedEvent> cleanedStream = rawStream
    .map(new SchemaValidator())
    .map(new DataStandardizer())
    .map(new EventEnricher());

cleanedStream.addSink(new IcebergSink(silverTable));
```

**Deliverables:**
- [ ] Flink job: `SilverProcessingJob.java`
- [ ] Schema Registry integration
- [ ] Iceberg Sink connector
- [ ] Monitoring & alerting (Flink metrics)

---

## Phase 4: Consent Management System

### 4.1 Consent Rule API

**Endpoints:**
- `POST /api/consent/rules` - Create/Update consent rule
- `GET /api/consent/rules/{athleteId}` - Fetch active rule
- `DELETE /api/consent/rules/{athleteId}/{purpose}` - Revoke purpose

**Rule Structure (JSON):**
```json
{
  "userId": "athlete-123",
  "dimensions": {
    "purpose": {
      "operator": "IN",
      "values": [{"value": "research"}, {"value": "education"}]
    },
    "activity": {
      "operator": "IN",
      "values": [{"value": "running"}, {"value": "cycling"}]
    },
    "event_context": {
      "operator": "EQUALS",
      "values": [{"value": "marathon_2025"}]
    },
    "time": {
      "operator": "BETWEEN",
      "values": [
        {"value": "2025-02-01", "type": "date"},
        {"value": "2025-12-31", "type": "date"}
      ]
    },
    "metric_conditions": {
      "heart_rate": {"operator": "LT", "value": 95}
    }
  }
}
```

**Deliverables:**
- [ ] `ConsentRuleController`
- [ ] `ConsentRuleService` (CRUD operations)
- [ ] Redis integration (Cache rules)
- [ ] Kafka event publisher (`consent.changed` topic)

---

### 4.2 Policy Engine (OPA Integration)

**Purpose:** Evaluate complex N*N rules at runtime.

**OPA Policy Example:**
```rego
package consent

allow {
    input.purpose == "research"
    input.activity == "running"
    input.heart_rate < 95
    input.timestamp >= "2025-02-01"
}
```

**Alternative:** Custom Java Rule Evaluator using the JSON structure.

**Deliverables:**
- [ ] OPA deployment (Docker container)
- [ ] Policy files (`.rego`)
- [ ] Java client for OPA REST API
- [ ] OR: Custom `RuleEvaluator.java`

---

## Phase 5: Gold Layer (Consent Enforcement & Physical Partitioning)

### 5.1 Consent Enforcer Job (Flink)

**The "Fan-Out" Logic:**
```java
DataStream<StandardizedEvent> silverStream = /* Read from Silver Iceberg */;

silverStream
    .keyBy(event -> event.getAthleteId())
    .process(new ConsentEnforcerFunction())
    .addSink(new MultiPartitionIcebergSink());

class ConsentEnforcerFunction extends KeyedProcessFunction {
    void processElement(StandardizedEvent event, Context ctx, Collector<PartitionedEvent> out) {
        // 1. Fetch consent rule from Redis
        ConsentRule rule = redis.get("consent:rule:" + event.getAthleteId());
        
        // 2. Evaluate against all purposes
        for (Purpose purpose : rule.getAllowedPurposes()) {
            if (policyEngine.evaluate(event, purpose, rule)) {
                // 3. Emit to specific partition
                out.collect(new PartitionedEvent(
                    purpose,
                    event.getActivity(),
                    event.getEventContext(),
                    event.getTimestamp(),
                    event
                ));
            }
        }
    }
}
```

**Iceberg Partition Spec:**
```java
PartitionSpec.builderFor(schema)
    .identity("purpose")
    .identity("activity")
    .identity("event_context")
    .day("timestamp")
    .build();
```

**Deliverables:**
- [ ] `ConsentEnforcerJob.java` (Flink)
- [ ] `PolicyEngine` integration
- [ ] Iceberg Gold table creation
- [ ] Partition spec configuration

---

### 5.2 Gold Table Schema

```sql
CREATE TABLE gold_athlete_data (
    trace_id STRING,
    athlete_id STRING,
    purpose STRING,           -- Partition key
    activity STRING,          -- Partition key
    event_context STRING,     -- Partition key
    timestamp TIMESTAMP,      -- Partition key (day)
    sport STRING,
    heart_rate DOUBLE,
    blood_pressure_sys DOUBLE,
    blood_pressure_dia DOUBLE,
    oxygen_sat DOUBLE,
    power_watts DOUBLE,
    gps_lat DOUBLE,
    gps_lon DOUBLE,
    raw_metrics MAP<STRING, STRING>,  -- Flexible for unknown metrics
    consent_version INT
) USING iceberg
PARTITIONED BY (purpose, activity, event_context, days(timestamp));
```

**Deliverables:**
- [ ] Iceberg table DDL
- [ ] Schema evolution plan (adding new metrics)

---

## Phase 6: Serving Layer (Query API)

### 6.1 Buyer Query API

**Endpoint:**
```
GET /api/query?purpose={purpose}&activity={activity}&event={event}&startDate={date}&endDate={date}
```

**Implementation:**
```java
@RestController
public class QueryController {
    
    @Autowired
    private TrinoClient trino;
    
    @GetMapping("/api/query")
    public ResponseEntity<QueryResponse> query(
        @RequestParam String purpose,
        @RequestParam(required = false) String activity,
        @RequestParam(required = false) String event,
        @RequestParam(required = false) String startDate,
        @RequestParam(required = false) String endDate
    ) {
        // Build SQL dynamically
        String sql = buildQuery(purpose, activity, event, startDate, endDate);
        
        // Execute via Trino
        List<DataItem> results = trino.execute(sql);
        
        return ResponseEntity.ok(new QueryResponse(results));
    }
    
    private String buildQuery(...) {
        return String.format(
            "SELECT * FROM gold_athlete_data WHERE purpose = '%s' " +
            "AND activity = '%s' AND timestamp BETWEEN '%s' AND '%s'",
            purpose, activity, startDate, endDate
        );
    }
}
```

**Deliverables:**
- [ ] `QueryController`
- [ ] Trino JDBC client integration
- [ ] Query builder (SQL generation)
- [ ] Response serialization

---

## Phase 7: Dynamic Re-Indexing

### 7.1 Consent Change Handler

**Trigger:** Kafka event on `consent.changed` topic.

**Backfill Worker:**
```java
@KafkaListener(topics = "consent.changed")
public void handleConsentChange(ConsentChangeEvent event) {
    if (event.getAction() == Action.REVOKE) {
        // Delete/Move Gold partition
        icebergCatalog.dropPartition(
            "gold_athlete_data",
            "purpose=" + event.getPurpose() + "/athlete_id=" + event.getAthleteId()
        );
    } else if (event.getAction() == Action.GRANT) {
        // Trigger backfill job
        backfillService.reprocessSilver(event.getAthleteId(), event.getPurpose());
    }
}
```

**Deliverables:**
- [ ] `ConsentChangeListener`
- [ ] `BackfillService` (Spark/Flink job)
- [ ] Partition deletion logic
- [ ] Monitoring (backfill progress)

---

## Phase 8: Advanced Features

### 8.1 Buyer-Specific Data Marts

**For high-value buyers (e.g., MegaCorp):**
```sql
CREATE TABLE gold_megacorp
USING iceberg
PARTITIONED BY (sport, season, player_id)
AS SELECT * FROM gold_athlete_data
WHERE buyer_list CONTAINS 'MegaCorp';
```

**Continuous Sync:**
- Flink job reads from Main Gold
- Filters for MegaCorp criteria
- Writes to `gold_megacorp` with custom partitioning

**Deliverables:**
- [ ] Data Mart creation scripts
- [ ] Sync job (Flink)
- [ ] Buyer-specific API endpoints

---

### 8.2 Partition Evolution Support

**Scenario:** Add `device_type` dimension later.

```sql
ALTER TABLE gold_athlete_data ADD COLUMN device_type STRING;
ALTER TABLE gold_athlete_data SET PARTITION SPEC ADD identity(device_type);
```

**Deliverables:**
- [ ] Migration scripts
- [ ] Documentation for partition evolution
- [ ] Testing (hybrid layout queries)

---

## Phase 9: Testing & Validation

### 9.1 Integration Tests

**Test Scenarios:**
1. **Multi-Source Ingestion**: API + Kafka + File upload
2. **N*N Consent Enforcement**:
   - Athlete allows "Research + Running + HR<95"
   - Verify only matching data in Gold
3. **Partition Pruning**: Query performance (should skip irrelevant partitions)
4. **Consent Revocation**: Immediate data removal
5. **Backfill**: Re-grant consent, verify historical data reappears

**Deliverables:**
- [ ] `IntegrationTestSuite.java`
- [ ] Test data generators
- [ ] Performance benchmarks

---

### 9.2 Load Testing

**Targets:**
- **Ingestion**: 10,000 events/second
- **Query Latency**: < 100ms (P95)
- **Concurrent Users**: 1,000 buyers

**Tools:**
- JMeter / Gatling for API load testing
- Kafka performance testing

**Deliverables:**
- [ ] Load test scripts
- [ ] Performance report

---

## Phase 10: Deployment & Operations

### 10.1 Production Deployment

**Infrastructure:**
- Kubernetes cluster (EKS/GKE) or Docker Swarm
- S3 (replace MinIO) for production storage
- Managed Kafka (Confluent Cloud / MSK)
- Monitoring: Prometheus + Grafana

**Deliverables:**
- [ ] Kubernetes manifests
- [ ] CI/CD pipeline (GitHub Actions / Jenkins)
- [ ] Secrets management (Vault)

---

### 10.2 Monitoring & Alerting

**Key Metrics:**
- Ingestion lag (Kafka consumer lag)
- Flink job backpressure
- Query latency (P50, P95, P99)
- Consent rule cache hit rate
- Backfill job duration

**Deliverables:**
- [ ] Grafana dashboards
- [ ] Alert rules (PagerDuty integration)

---

## Implementation Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1: Infrastructure | 1 week | None |
| Phase 2: Ingestion | 1 week | Phase 1 |
| Phase 3: Silver Processing | 2 weeks | Phase 1, 2 |
| Phase 4: Consent Management | 2 weeks | Phase 1 |
| Phase 5: Gold Layer | 3 weeks | Phase 3, 4 |
| Phase 6: Serving API | 1 week | Phase 5 |
| Phase 7: Re-Indexing | 2 weeks | Phase 5, 6 |
| Phase 8: Advanced Features | 2 weeks | Phase 7 |
| Phase 9: Testing | 2 weeks | All phases |
| Phase 10: Deployment | 1 week | Phase 9 |

**Total Estimated Duration:** 17 weeks (~4 months)

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Iceberg learning curve | Medium | Allocate 1 week for POC |
| OPA complexity | Medium | Consider custom evaluator as fallback |
| Backfill performance | High | Implement incremental backfill |
| Storage costs (duplication) | Medium | Monitor Gold size, implement TTL policies |
| Flink job failures | High | Implement checkpointing, alerting |

---

## Success Criteria

- [ ] System handles 1M athletes with 10K events/sec
- [ ] Query latency < 100ms for 95% of requests
- [ ] Consent revocation takes effect within 1 minute
- [ ] Zero data leakage (unconsented data never served)
- [ ] Support for 8+ consent dimensions
- [ ] Partition evolution works without downtime

---

## Next Steps

1. **Review this plan** with stakeholders
2. **Approve technology stack** (Iceberg, Flink, Trino, OPA)
3. **Set up development environment** (Phase 1)
4. **Create POC** for Iceberg + Flink integration
5. **Begin Phase 2** implementation
