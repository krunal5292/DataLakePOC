# Kafka Connection Issue - Troubleshooting Summary
**Date:** January 2, 2026  
**Issue:** Manual tests failing to connect to Kafka while Testcontainers tests work fine

---

## Problem Statement

The manual test `DataFlowRevocationManualTest` was unable to connect to Kafka at `localhost:9092`, resulting in:
- `TimeoutException: Timed out waiting for a node assignment`
- `KafkaAdmin` unable to create/check topics
- Test hanging indefinitely waiting for Kafka connection
- `OutOfMemoryError: Cannot reserve bytes of direct buffer memory` (secondary issue)

Meanwhile, `DataFlowKafkaTestcontainersTest` worked perfectly fine.

---

## Root Cause Analysis

### Investigation Steps

1. **Verified Kafka Infrastructure**
   - ✅ Docker Compose services running
   - ✅ Kafka container healthy
   - ✅ Port 9092 accessible via `nc -zv localhost 9092`
   - ✅ Kafka broker API responding from within Docker network

2. **Identified Port Conflict**
   ```bash
   $ lsof -i :9092 | grep LISTEN
   Electron    465  - TCP localhost:9092 (LISTEN)  # IDE process
   com.docker 2040  - TCP *:9092 (LISTEN)          # Kafka
   ```
   
   **Root Cause:** Port 9092 was being used by both:
   - IntelliJ IDEA (Electron process, PID 465)
   - Docker Kafka (PID 2040)

3. **Why Testcontainers Worked**
   - Testcontainers uses `@DynamicPropertySource` to override bootstrap servers
   - Creates isolated containers with dynamic port mapping
   - No port conflicts in isolated environment

---

## Solution Implemented

### 1. Changed Kafka Port from 9092 to 9093

**File:** `docker-compose.yml`
```yaml
kafka:
  ports:
    - "9093:9092"  # Changed from 9092:9092
  environment:
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9093,PLAINTEXT_INTERNAL://kafka:29092
```

**File:** `application.properties`
```properties
spring.kafka.bootstrap-servers=localhost:9093
```

### 2. Added Kafka Health Checks

**File:** `docker-compose.yml`
```yaml
zookeeper:
  healthcheck:
    test: ["CMD", "nc", "-z", "localhost", "2181"]
    interval: 10s
    timeout: 5s
    retries: 5

kafka:
  depends_on:
    zookeeper:
      condition: service_healthy
  environment:
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
  healthcheck:
    test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
    interval: 10s
    timeout: 10s
    retries: 5
```

### 3. Configured Logging

**File:** `application.properties`
```properties
# Logging configuration
logging.level.org.apache.kafka=INFO
logging.level.org.apache.kafka.clients.NetworkClient=ERROR
logging.level.org.springframework.kafka=INFO
logging.pattern.console=%d{yyyy-MM-dd'T'HH:mm:ss.SSSXXX} %5p --- [%15.15t] %-40.40logger{39} : %m%n
```

### 4. Converted Manual Test to Debug-Friendly Format

**File:** `DataFlowRevocationManualTest.java`

**Before:**
```java
System.out.println("Press ENTER to continue...");
scanner.nextLine();  // Blocks waiting for input
```

**After:**
```java
logger.info("=== BREAKPOINT 1: Data Ingested ===");
logger.info(">> CHECK MINIO: Verify 'gold/active' contains files for athlete: {}", athleteId);
logger.info("   Set breakpoint here and inspect MinIO at http://localhost:9001");
// Breakpoint location - check MinIO before continuing
```

Added **3 breakpoint locations** for debugging:
1. After data ingestion
2. After consent revocation
3. After consent re-grant (replay)

---

## Verification Results

### ✅ Infrastructure Status
```bash
$ docker-compose ps
kafka    healthy   0.0.0.0:9093->9092/tcp
```

### ✅ Kafka Connectivity Test
```bash
$ docker run --rm --network host confluentinc/cp-kafka:7.5.0 \
  kafka-broker-api-versions --bootstrap-server localhost:9093
localhost:9093 (id: 1 rack: null) -> (
  Produce(0): 0 to 9 [usable: 9],
  Fetch(1): 0 to 15 [usable: 15],
  ...
)
```

### ✅ Manual Test Results
- ✅ Kafka Admin connected successfully
- ✅ All Kafka consumers started (bronze-group, silver-group, gold-group, gold-index-manager)
- ✅ Data ingestion working (13 rows)
- ✅ Bronze layer processing complete
- ✅ Silver layer processing complete
- ✅ Gold layer consent-based fan-out working
- ✅ No OutOfMemoryError
- ✅ No connection timeouts

---

## Files Modified

| File | Changes |
|------|---------|
| `docker-compose.yml` | Changed Kafka port to 9093, added health checks, added inter-broker listener config |
| `application.properties` | Updated bootstrap server to localhost:9093, added logging configuration |
| `DataFlowRevocationManualTest.java` | Removed Scanner interactions, added logger breakpoint markers |

---

## How to Run Manual Test

### Option 1: Debug Mode (Recommended)
1. Open `DataFlowRevocationManualTest.java` in IntelliJ
2. Set breakpoints on the three `// Breakpoint location` comment lines
3. Right-click → **Debug 'DataFlowRevocationManualTest'**
4. At each breakpoint:
   - Open MinIO at http://localhost:9001 (admin/password)
   - Verify data as instructed in logs
   - Press F9 to continue

### Option 2: Command Line
```bash
./mvnw test -Dtest=DataFlowRevocationManualTest -Dspring.profiles.active=manual
```

---

## Key Learnings

1. **Port conflicts can cause mysterious connection issues** - Always check `lsof -i :PORT` when debugging connectivity
2. **Testcontainers isolates tests** - That's why it worked while manual tests failed
3. **Health checks ensure proper startup order** - Prevents race conditions between Zookeeper and Kafka
4. **Interactive tests don't work well in CI/CD** - Converting to debug-friendly format is better
5. **Advertised listeners are critical** - Must match the actual accessible endpoint from client perspective

---

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka | localhost:9093 | - |
| Kafka UI | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | admin/password |
| ClickHouse HTTP | localhost:8123 | default/password |
| Redis | localhost:6379 | - |

---

## Next Steps

- [ ] Consider adding automated tests instead of manual verification
- [ ] Document the debugging workflow for future reference
- [ ] Add port conflict detection to startup scripts
- [ ] Consider using random ports for local development to avoid conflicts

---

## Conclusion

The issue was **NOT** a Kafka configuration problem or memory issue - it was a simple **port conflict** between the IDE and Docker. Changing Kafka to port 9093 resolved all connectivity issues, and the manual test now works perfectly with debug breakpoints for easy verification.
