package org.example.manual;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.model.GoldDataResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Manual Test for Consent-Aware Read Path
 * 
 * This test runs against LOCAL Docker infrastructure (docker-compose).
 * It replicates the logic from DataFlowReadPathTestcontainersTest but uses
 * the persistent local environment instead of Testcontainers.
 * 
 * Prerequisites:
 * 1. Start infrastructure: docker-compose up -d
 * 2. Run with profile: mvn test -Dtest=DataFlowReadPathManualTest
 * -Dspring.profiles.active=manual
 */
@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DataFlowReadPathManualTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ResourceLoader resourceLoader;

    @Test
    void testEndToEndConsentEnforcement() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("🚀 MANUAL TEST: Consent-Aware Read Path Verification");
        System.out.println("=".repeat(80) + "\n");

        // 1. SETUP: Load Consent Rule and Extract Purposes Dynamically
        String athleteId = "550e8400-e29b-41d4-a716-446655440000"; // ID from JSON
        String consentKey = "consent:rule:" + athleteId;

        ComplexConsentRule rule;
        List<String> consentedPurposes = new java.util.ArrayList<>();
        List<String> unconsentedPurposes = List.of("marketing", "advertising", "thirdPartySales");

        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0);

            // Dynamically extract all consented purposes from the rule
            if (rule.getDimensions().getPurpose() != null
                    && rule.getDimensions().getPurpose().getValues() != null) {
                for (ComplexConsentRule.ValueDetail purpose : rule.getDimensions().getPurpose().getValues()) {
                    consentedPurposes.add(purpose.getValue());
                }
            }

            System.out.println("📋 Consented Purposes: " + consentedPurposes);
            System.out.println("🚫 Unconsented Purposes to Test: " + unconsentedPurposes);

            redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));
            System.out.println("✅ Consent rule loaded into Redis\n");
        }

        // 2. INGEST: Send Telemetry Data
        System.out.println("📤 INGESTING DATA...");
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

        // Send 3 events
        int eventCount = 3;
        for (int i = 0; i < eventCount; i++) {
            Map<String, Object> row = dataRows.get(0);
            row.put("athlete_id", athleteId);
            row.put("trace_id", UUID.randomUUID().toString());
            restTemplate.postForEntity("/api/ingest", row, Map.class);
            System.out.println("  ✅ Event " + (i + 1) + " sent");
        }

        // 3. WAIT: Allow Processing Pipeline to Finish (Bronze -> Silver -> Gold)
        System.out.println("\n⏳ WAITING for pipeline processing (Bronze → Silver → Gold)...");
        String firstConsentedPurpose = consentedPurposes.get(0);
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
                    "/api/query?athleteId={id}&purposes={p}",
                    GoldDataResponse.class,
                    athleteId,
                    firstConsentedPurpose);
            return response.getStatusCode() == HttpStatus.OK
                    && response.getBody() != null
                    && response.getBody().getData() != null
                    && response.getBody().getData().size() >= eventCount;
        });
        System.out.println("✅ Pipeline processing complete!\n");

        // 4. VERIFY: Access Control - DYNAMIC VERIFICATION

        // CASE A: Verify ALL Consented Purposes -> Should Return Data
        System.out.println("🔍 VERIFYING CONSENTED PURPOSES:");
        System.out.println("-".repeat(80));
        for (String purpose : consentedPurposes) {
            ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
                    "/api/query?athleteId={id}&purposes={p}",
                    GoldDataResponse.class,
                    athleteId,
                    purpose);

            assertEquals(HttpStatus.OK, response.getStatusCode(),
                    "Query for consented purpose '" + purpose + "' should return OK");

            assertNotNull(response.getBody());
            List<org.example.query.model.DataItem> data = response.getBody().getData();

            assertNotNull(data, "Consented purpose '" + purpose + "' should have data");
            assertFalse(data.isEmpty(), "Consented purpose '" + purpose + "' should have data");
            assertTrue(data.size() >= eventCount,
                    "Consented purpose '" + purpose + "' should have at least " + eventCount + " records");

            // STRICT VERIFICATION: verify each data item, one by one
            for (org.example.query.model.DataItem item : data) {
                assertEquals(purpose, item.getPurpose(),
                        "Found data with incorrect purpose! Expected '" + purpose + "' but got '" + item.getPurpose()
                                + "'");

                assertNotNull(item.getContent(), "Content for purpose '" + purpose + "' should not be null");
                assertFalse(item.getContent().isEmpty(), "Content for purpose '" + purpose + "' should not be empty");
            }

            System.out.println("  ✅ Purpose '" + purpose + "': Verified " + data.size() + " records");
        }

        // CASE B: Verify ALL Unconsented Purposes -> Should Return EMPTY Data
        System.out.println("\n🔍 VERIFYING UNCONSENTED PURPOSES:");
        System.out.println("-".repeat(80));
        for (String purpose : unconsentedPurposes) {
            ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
                    "/api/query?athleteId={id}&purposes={p}",
                    GoldDataResponse.class,
                    athleteId,
                    purpose);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertTrue(response.getBody().getData().isEmpty(),
                    "Unconsented purpose '" + purpose + "' should have NO data");

            System.out.println("  ✅ Purpose '" + purpose + "': Correctly returns NO data");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("✅ VERIFICATION COMPLETE: All " + consentedPurposes.size()
                + " consented purposes return data, all " + unconsentedPurposes.size()
                + " unconsented purposes return nothing.");
        System.out.println("=".repeat(80) + "\n");
    }
}
