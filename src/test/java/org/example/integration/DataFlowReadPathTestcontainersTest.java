package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.awaitility.Awaitility;
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

import java.io.InputStream;
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

        // Create output directory for manual inspection
        java.nio.file.Path outputDir = java.nio.file.Paths.get("target/test-output");
        java.nio.file.Files.createDirectories(outputDir);

        // Consolidated response for athlete (all consented data)
        Map<String, Object> athleteConsentedData = new java.util.LinkedHashMap<>();
        athleteConsentedData.put("athleteId", athleteId);
        athleteConsentedData.put("consentedPurposes", consentedPurposes);
        Map<String, List<org.example.query.model.DataItem>> dataByPurpose = new java.util.LinkedHashMap<>();

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

            // Collect data for consolidated output
            dataByPurpose.put(purpose, data);

            System.out.println("  ✅ Purpose '" + purpose + "': Verified " + data.size() + " records");
        }

        // Save consolidated consented data for athlete
        athleteConsentedData.put("dataByPurpose", dataByPurpose);
        athleteConsentedData.put("totalRecords", dataByPurpose.values().stream().mapToInt(List::size).sum());

        String consentedFilename = String.format("athlete_%s_consented_data.json", athleteId.substring(0, 8));
        java.nio.file.Path consentedFile = outputDir.resolve(consentedFilename);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(consentedFile.toFile(), athleteConsentedData);

        System.out.println("\n📄 Consolidated consented data saved to: " + consentedFile);

        // CASE B: Verify ALL Unconsented Purposes -> Should Return EMPTY Data
        Map<String, Object> athleteUnconsentedData = new java.util.LinkedHashMap<>();
        athleteUnconsentedData.put("athleteId", athleteId);
        athleteUnconsentedData.put("unconsentedPurposes", unconsentedPurposes);
        Map<String, List<org.example.query.model.DataItem>> unconsentedResults = new java.util.LinkedHashMap<>();

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

            unconsentedResults.put(purpose, response.getBody().getData());
            System.out.println("  ✅ Purpose '" + purpose + "': Correctly returns NO data");
        }

        // Save unconsented verification results
        athleteUnconsentedData.put("results", unconsentedResults);
        athleteUnconsentedData.put("verification", "All unconsented purposes correctly return empty data");

        String unconsentedFilename = String.format("athlete_%s_unconsented_verification.json",
                athleteId.substring(0, 8));
        java.nio.file.Path unconsentedFile = outputDir.resolve(unconsentedFilename);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(unconsentedFile.toFile(), athleteUnconsentedData);

        System.out.println("📄 Unconsented verification saved to: " + unconsentedFile);

        System.out.println("\n✅ VERIFICATION COMPLETE: All " + consentedPurposes.size()
                + " consented purposes return data, all " + unconsentedPurposes.size()
                + " unconsented purposes return nothing.");
        System.out.println("📂 All responses saved to: " + outputDir.toAbsolutePath());
    }
}
