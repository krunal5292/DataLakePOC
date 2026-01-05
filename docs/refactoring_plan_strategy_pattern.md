# Refactoring Plan: Strategy Pattern Integration

**Date:** January 05, 2026  
**Objective:** Make existing Bitmask implementation compatible with pluggable Gold Strategy pattern

---

## Current State Analysis

### ✅ What's Already Good (Shared Foundation)

**Bronze Layer:**
- ✅ `IngestionController` - Generic ingestion API
- ✅ MinIO Bronze bucket - Raw data storage
- ✅ Kafka producer - Event streaming

**Silver Layer:**
- ✅ `SilverService` - Data cleaning/standardization
- ✅ MinIO Silver bucket - Cleaned data storage

**Consent Management:**
- ✅ `ConsentController` - CRUD API
- ✅ Redis integration - Rule caching
- ✅ `ComplexConsentRule` model - N*N rule structure

### 🔧 What Needs Refactoring

**Gold Layer:**
- Current: `GoldService` directly writes to MinIO with physical partitioning
- Target: Extract into `PhysicalPartitionStrategy` implementing `GoldEnforcementStrategy`

**Test Cases:**
All 3 tests currently assume physical partitioning. Need to make them strategy-agnostic.

---

## Test Case Analysis

### Test 1: DataFlowKafkaTestcontainersTest
**What it tests:**
- End-to-end flow: Ingest → Bronze → Silver → Gold
- Verifies data appears in Gold for consented purposes
- Verifies unconsented purposes have NO data

**Current Gold verification:**
```java
private boolean checkGoldPathExists(String purpose, String athleteId) {
    // Looks for: "purpose/{purpose}" AND "athlete_id={athleteId}"
    // This is PHYSICAL PARTITION specific
}
```

**Strategy compatibility:**
- ✅ **Physical Partition**: Works as-is
- ❌ **Bitmask**: Would need different verification (query database, not file paths)

---

### Test 2: DataFlowReadPathTestcontainersTest
**What it tests:**
- Query API (`/api/query?purposes={purpose}`)
- Verifies consented purposes return data
- Verifies unconsented purposes return empty

**Current verification:**
```java
ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
    "/api/query?athleteId={id}&purposes={p}",
    GoldDataResponse.class, athleteId, purpose);

assertThat(response.getBody().getData()).isNotEmpty();
```

**Strategy compatibility:**
- ✅ **Physical Partition**: Works (current implementation)
- ✅ **Bitmask**: Would work IF query API is strategy-aware
- ✅ **This test is already strategy-agnostic!** (Uses API, not direct storage)

---

### Test 3: DataFlowRevocationTestcontainersTest
**What it tests:**
- Consent revocation cycle
- Data moves from `active/` to `history/`
- Re-grant restores data to `active/`

**Current verification:**
```java
// Checks MinIO paths: "active/" vs "history/"
// This is PHYSICAL PARTITION specific
```

**Strategy compatibility:**
- ✅ **Physical Partition**: Works as-is
- ❌ **Bitmask**: Would need different approach (update bitmask, not move files)

---

## Refactoring Strategy

### Phase 1: Create Strategy Interface

```java
package org.example.processing.strategy;

public interface GoldEnforcementStrategy {
    
    /**
     * Process Silver data and write to Gold based on consent
     */
    void processAndFanOut(TelemetryMessage message);
    
    /**
     * Handle consent revocation
     */
    void handleRevocation(String athleteId, String purpose);
    
    /**
     * Handle consent re-grant (backfill from Silver)
     */
    void handleGrant(String athleteId, String purpose);
    
    /**
     * Verify data exists for a purpose (for testing)
     */
    boolean verifyDataExists(String athleteId, String purpose);
    
    /**
     * Get strategy name
     */
    String getStrategyName();
}
```

**Deliverables:**
- [ ] Create `GoldEnforcementStrategy` interface
- [ ] Create `strategy` package

---

### Phase 2: Extract Physical Partition Strategy

**Refactor existing `GoldService` into strategy:**

```java
package org.example.processing.strategy;

@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "partition", matchIfMissing = true)
public class PhysicalPartitionStrategy implements GoldEnforcementStrategy {
    
    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    
    @Override
    public void processAndFanOut(TelemetryMessage message) {
        // MOVE existing GoldService.processAndFanOut() logic here
    }
    
    @Override
    public void handleRevocation(String athleteId, String purpose) {
        // Move files from active/ to history/
        String activePath = String.format("active/*/purpose/%s/athlete_id=%s/", purpose, athleteId);
        String historyPath = String.format("history/%s/purpose/%s/athlete_id=%s/", 
            LocalDate.now(), purpose, athleteId);
        
        // Copy and delete logic
    }
    
    @Override
    public void handleGrant(String athleteId, String purpose) {
        // Trigger backfill from Silver
        // Read Silver data for athlete
        // Re-run processAndFanOut for specific purpose
    }
    
    @Override
    public boolean verifyDataExists(String athleteId, String purpose) {
        // Check MinIO path exists
        Iterable<Result<Item>> results = minioClient.listObjects(...);
        for (Result<Item> result : results) {
            if (result.get().objectName().contains("purpose/" + purpose) 
                && result.get().objectName().contains("athlete_id=" + athleteId)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String getStrategyName() {
        return "partition";
    }
}
```

**Deliverables:**
- [ ] Create `PhysicalPartitionStrategy`
- [ ] Move logic from `GoldService`
- [ ] Add `@ConditionalOnProperty` annotation

---

### Phase 3: Create Bitmask Strategy (Placeholder)

```java
package org.example.processing.strategy;

@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "bitmask")
public class BitmaskStrategy implements GoldEnforcementStrategy {
    
    // For now, just a placeholder that throws UnsupportedOperationException
    // Will implement in Phase 2 of actual development
    
    @Override
    public void processAndFanOut(TelemetryMessage message) {
        throw new UnsupportedOperationException("Bitmask strategy not yet implemented");
    }
    
    @Override
    public boolean verifyDataExists(String athleteId, String purpose) {
        // Query database with bitmask filter
        // For now, return false
        return false;
    }
    
    @Override
    public String getStrategyName() {
        return "bitmask";
    }
}
```

**Deliverables:**
- [ ] Create `BitmaskStrategy` skeleton
- [ ] Add TODO comments for future implementation

---

### Phase 4: Update GoldService to Delegate

```java
@Service
public class GoldService {
    
    private final GoldEnforcementStrategy strategy;
    
    @Autowired
    public GoldService(GoldEnforcementStrategy strategy) {
        this.strategy = strategy;
        System.out.println("🎯 Using Gold Strategy: " + strategy.getStrategyName());
    }
    
    public void processAndFanOut(TelemetryMessage message) {
        strategy.processAndFanOut(message);
    }
    
    public void handleRevocation(String athleteId, String purpose) {
        strategy.handleRevocation(athleteId, purpose);
    }
    
    public void handleGrant(String athleteId, String purpose) {
        strategy.handleGrant(athleteId, purpose);
    }
}
```

**Deliverables:**
- [ ] Refactor `GoldService` to be a facade
- [ ] Auto-inject strategy based on config

---

### Phase 5: Update Configuration

**application.yml:**
```yaml
consent:
  enforcement:
    strategy: "partition"  # or "bitmask"

# For partition strategy
minio:
  bucket:
    gold: "gold"
    
# For bitmask strategy (future)
clickhouse:
  url: "jdbc:clickhouse://localhost:8123/default"
```

**Deliverables:**
- [ ] Add strategy config property
- [ ] Document both strategies

---

### Phase 6: Make Tests Strategy-Aware

#### Option A: Parameterized Tests (Run both strategies)

```java
@ParameterizedTest
@ValueSource(strings = {"partition", "bitmask"})
public void testFullKafkaFlow(String strategy) {
    // Set system property
    System.setProperty("consent.enforcement.strategy", strategy);
    
    // Restart context
    context.refresh();
    
    // Run test
    // ...
}
```

#### Option B: Strategy-Specific Verification Helper

```java
@Autowired
private GoldEnforcementStrategy strategy;

private boolean verifyGoldDataExists(String athleteId, String purpose) {
    // Delegate to strategy
    return strategy.verifyDataExists(athleteId, purpose);
}
```

**Recommended: Option B** (simpler, works with current test structure)

**Refactor Test 1 (Kafka):**
```java
@Autowired
private GoldEnforcementStrategy goldStrategy;  // Add this

private boolean checkGoldPathExists(String purpose, String athleteId) throws Exception {
    // OLD: Direct MinIO check
    // NEW: Delegate to strategy
    return goldStrategy.verifyDataExists(athleteId, purpose);
}
```

**Refactor Test 3 (Revocation):**
```java
// Instead of checking MinIO paths directly, use strategy verification
Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
    // OLD: Check active/ path is empty
    // NEW: Verify data doesn't exist via strategy
    return !goldStrategy.verifyDataExists(athleteId, testPurpose);
});
```

**Test 2 (Read Path):**
- ✅ Already strategy-agnostic! No changes needed.

**Deliverables:**
- [ ] Update Test 1: Use `strategy.verifyDataExists()`
- [ ] Update Test 3: Use strategy verification
- [ ] Test 2: No changes needed

---

## Implementation Phases

### Phase 1: Interface & Extraction (1 day)
- [ ] Create `GoldEnforcementStrategy` interface
- [ ] Extract `PhysicalPartitionStrategy` from `GoldService`
- [ ] Update `GoldService` to delegate

### Phase 2: Configuration & Wiring (0.5 days)
- [ ] Add config properties
- [ ] Add `@ConditionalOnProperty` annotations
- [ ] Test strategy auto-injection

### Phase 3: Test Refactoring (1 day)
- [ ] Update Test 1 (Kafka)
- [ ] Update Test 3 (Revocation)
- [ ] Run all tests with `strategy=partition`

### Phase 4: Bitmask Skeleton (0.5 days)
- [ ] Create `BitmaskStrategy` placeholder
- [ ] Document future implementation plan

**Total: 3 days**

---

## Test Compatibility Matrix

| Test | Current | After Refactor (Partition) | After Refactor (Bitmask) |
|------|---------|---------------------------|-------------------------|
| **Kafka Flow** | ✅ Works | ✅ Works (via strategy) | ⚠️ Needs Bitmask impl |
| **Read Path** | ✅ Works | ✅ Works (no changes) | ✅ Works (API-based) |
| **Revocation** | ✅ Works | ✅ Works (via strategy) | ⚠️ Needs Bitmask impl |

---

## Migration Path

### Step 1: Refactor without breaking (This PR)
- Extract strategy pattern
- All tests still pass with `strategy=partition`
- No functional changes

### Step 2: Implement Bitmask (Future PR)
- Add ClickHouse dependency
- Implement `BitmaskStrategy.processAndFanOut()`
- Implement `BitmaskStrategy.verifyDataExists()`
- Run tests with `strategy=bitmask`

### Step 3: Comparison (Future)
- Run both strategies in parallel
- Performance benchmarks
- Feature comparison
- Choose primary strategy

---

## File Changes Summary

### New Files:
- `src/main/java/org/example/processing/strategy/GoldEnforcementStrategy.java`
- `src/main/java/org/example/processing/strategy/PhysicalPartitionStrategy.java`
- `src/main/java/org/example/processing/strategy/BitmaskStrategy.java`

### Modified Files:
- `src/main/java/org/example/processing/service/GoldService.java` (Refactor to facade)
- `src/test/java/org/example/integration/DataFlowKafkaTestcontainersTest.java` (Use strategy)
- `src/test/java/org/example/integration/DataFlowRevocationTestcontainersTest.java` (Use strategy)
- `src/main/resources/application.yml` (Add strategy config)

### Unchanged Files:
- `src/test/java/org/example/integration/DataFlowReadPathTestcontainersTest.java` ✅
- All Bronze/Silver layer code ✅
- All Consent management code ✅

---

## Success Criteria

- [ ] All 3 tests pass with `strategy=partition`
- [ ] Strategy is configurable via `application.yml`
- [ ] `GoldService` is a thin facade
- [ ] `PhysicalPartitionStrategy` contains all current logic
- [ ] `BitmaskStrategy` exists as placeholder
- [ ] No functional regressions
- [ ] Code is cleaner and more maintainable

---

## Next Steps

1. **Review this refactoring plan**
2. **Approve the approach**
3. **Start Phase 1**: Create interface and extract strategy
4. **Run tests** to ensure no breakage
5. **Commit** with message: "Refactor: Extract Gold layer into Strategy Pattern"
