package org.example.manual;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.model.GoldDataResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DataFlowKafkaManualTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private org.example.query.service.GoldDataQueryService goldDataQueryService;

    @Test
    public void testFullKafkaFlow_Manual() throws Exception {
        String athleteId = "athlete-kafka-" + UUID.randomUUID().toString().substring(0, 8);

        // 1. Setup Complex Consent Rule from JSON (load array and use first rule)
        ComplexConsentRule rule;
        List<String> consentedPurposes = new java.util.ArrayList<>();

        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId); // Override ID for test isolation

            // Dynamically extract all consented purposes
            if (rule.getDimensions().getPurpose() != null
                    && rule.getDimensions().getPurpose().getValues() != null) {
                for (ComplexConsentRule.ValueDetail purpose : rule.getDimensions().getPurpose().getValues()) {
                    consentedPurposes.add(purpose.getValue());
                }
            }

            redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));
        }

        System.out.println("\n📋 Consented Purposes: " + consentedPurposes);

        // 2. Ingest Data from JSON
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

        int expectedRecordCount = dataRows.size(); // Dynamic count based on actual data
        System.out.println("📤 Ingesting " + expectedRecordCount + " records...\n");

        for (Map<String, Object> row : dataRows) {
            row.put("athlete_id", athleteId); // Override ID for test isolation
            restTemplate.postForEntity("/api/ingest", row, Map.class);
        }

        // 3. Await Processing and Verify All Layers
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            // Check Bronze
            boolean bronzeExists = checkBucketPathExists("bronze", athleteId);
            assertTrue(bronzeExists, "Bronze bucket should have data for athlete");

            // Check Silver
            boolean silverExists = checkBucketPathExists("silver", athleteId);
            assertTrue(silverExists, "Silver bucket should have data for athlete");

            // --- DYNAMIC Gold Layer Verification ---
            System.out.println("\n🔍 VERIFYING GOLD LAYER:");

            // Verify ALL consented purposes have AT LEAST the expected records
            for (String purpose : consentedPurposes) {
                int count = getGoldFileCount(purpose, athleteId);
                assertTrue(count >= expectedRecordCount,
                        "Purpose '" + purpose + "' should have at least " + expectedRecordCount
                                + " records (found " + count + ")");
                System.out.println("  ✅ Purpose '" + purpose + "': " + count + " records");
            }

            // Verify unconsented purpose has 0 records
            int marketingCount = getGoldFileCount("marketing", athleteId);
            assertEquals(0, marketingCount,
                    "Marketing (unconsented) should have 0 records (found " + marketingCount + ")");
            System.out.println("  ✅ Purpose 'marketing' (unconsented): 0 records");
        });

        // 4. DEMONSTRATE CONSENT-AWARE DATA QUERY
        System.out.println("\n=== CONSENT-AWARE DATA QUERY DEMONSTRATION ===");

        // Query all consented data
        GoldDataResponse allData = goldDataQueryService.getAllConsentedData(athleteId);
        System.out.println("Total consented purposes: " + allData.getApprovedPurposes().size());
        System.out.println("Total data records retrieved: " + allData.getTotalRecords());
        System.out.println("Approved purposes: " + allData.getApprovedPurposes());

        // Query specific purposes
        List<String> requestedPurposes = Arrays.asList("research", "education");
        GoldDataResponse specificData = goldDataQueryService.queryAthleteData(athleteId,
                requestedPurposes);
        System.out.println("\nQueried purposes: " + requestedPurposes);
        System.out.println("Records for requested purposes: " + specificData.getTotalRecords());

        // Validate consent for specific purpose
        boolean hasResearchConsent = goldDataQueryService.validateConsent(athleteId, "research");
        boolean hasMarketingConsent = goldDataQueryService.validateConsent(athleteId, "marketing");
        System.out.println("\nConsent validation:");
        System.out.println("  - Research: " + hasResearchConsent + " (should be true)");
        System.out.println("  - Marketing: " + hasMarketingConsent + " (should be false)");

        // Try to query non-consented purpose (should throw exception)
        System.out.println("\nTesting consent violation...");
        try {
            goldDataQueryService.queryAthleteData(athleteId, java.util.Arrays.asList("marketing"));
            System.out.println("ERROR: Should have thrown ConsentViolationException!");
        } catch (org.example.query.exception.ConsentViolationException e) {
            System.out.println("✓ Correctly blocked unauthorized access: " + e.getMessage());
        }

        System.out.println("\n=== CONSENT-AWARE QUERY TEST COMPLETE ===");
    }

    private int getGoldFileCount(String purpose, String athleteId) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("gold").build())) {
            return 0;
        }

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket("gold").recursive(true).build());

        int count = 0;
        for (Result<Item> result : results) {
            String name = result.get().objectName();
            // Path structure: gold/YYYY-MM-DD/purpose/PURPOSE/athlete_id=ID/...
            // We check if the path contains the specific purpose directory AND the athlete
            // ID
            if (name.contains("purpose/" + purpose + "/") && name.contains("athlete_id=" + athleteId)) {
                count++;
            }
        }
        return count;
    }

    private boolean checkBucketPathExists(String bucket, String athleteId) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
            return false;
        }
        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket(bucket).recursive(true).build());
        for (Result<Item> result : results) {
            if (result.get().objectName().contains(athleteId)) {
                return true;
            }
        }
        return false;
    }
}
