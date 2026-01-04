package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.model.GoldDataResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.awaitility.Awaitility;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = "gold.index.manager.group-id=read-path-test-group")
public class DataFlowReadPathTestcontainersTest extends BaseTestcontainersTest {

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

    @BeforeEach
    void setupMinio() throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("bronze").build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket("bronze").build());
            minioClient.makeBucket(MakeBucketArgs.builder().bucket("silver").build());
            minioClient.makeBucket(MakeBucketArgs.builder().bucket("gold").build());
        }
    }

    @Test
    void testEndToEndConsentEnforcement() throws Exception {
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
        }

        // 2. INGEST: Send Telemetry Data
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
        }

        // 3. WAIT: Allow Processing Pipeline to Finish (Bronze -> Silver -> Gold)
        // Wait for data to appear in at least one consented purpose
        String firstConsentedPurpose = consentedPurposes.get(0);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
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

        // 4. VERIFY: Access Control - DYNAMIC VERIFICATION

        // CASE A: Verify ALL Consented Purposes -> Should Return Data
        System.out.println("\n🔍 VERIFYING CONSENTED PURPOSES:");
        for (String purpose : consentedPurposes) {
            ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
                    "/api/query?athleteId={id}&purposes={p}",
                    GoldDataResponse.class,
                    athleteId,
                    purpose);

            assertThat(response.getStatusCode())
                    .as("Query for consented purpose '%s' should return OK", purpose)
                    .isEqualTo(HttpStatus.OK);

            assertThat(response.getBody()).isNotNull();
            List<org.example.query.model.DataItem> data = response.getBody().getData();

            assertThat(data)
                    .as("Consented purpose '%s' should have data", purpose)
                    .isNotEmpty()
                    .hasSizeGreaterThanOrEqualTo(eventCount);

            // STRICT VERIFICATION: verify each data item, one by one
            for (org.example.query.model.DataItem item : data) {
                assertThat(item.getPurpose())
                        .withFailMessage("Found data with incorrect purpose! Expected '%s' but got '%s'",
                                purpose, item.getPurpose())
                        .isEqualTo(purpose);

                assertThat(item.getContent())
                        .as("Content for purpose '%s' should not be empty", purpose)
                        .isNotEmpty();
            }

            System.out.println("  ✅ Purpose '" + purpose + "': Verified " + data.size() + " records");
        }

        // CASE B: Verify ALL Unconsented Purposes -> Should Return EMPTY Data
        System.out.println("\n🔍 VERIFYING UNCONSENTED PURPOSES:");
        for (String purpose : unconsentedPurposes) {
            ResponseEntity<GoldDataResponse> response = restTemplate.getForEntity(
                    "/api/query?athleteId={id}&purposes={p}",
                    GoldDataResponse.class,
                    athleteId,
                    purpose);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody().getData())
                    .as("Unconsented purpose '%s' should have NO data", purpose)
                    .isEmpty();

            System.out.println("  ✅ Purpose '" + purpose + "': Correctly returns NO data");
        }

        System.out.println("\n✅ VERIFICATION COMPLETE: All " + consentedPurposes.size()
                + " consented purposes return data, all " + unconsentedPurposes.size()
                + " unconsented purposes return nothing.");
    }
}
