package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.example.consent.model.ComplexConsentRule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataFlowKafkaTestcontainersTest extends BaseTestcontainersTest {

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

    @Test
    public void testFullKafkaFlow_Testcontainers() throws Exception {
        String athleteId = "athlete-tc-" + UUID.randomUUID().toString().substring(0, 8);

        // 1. Setup Complex Consent Rule from JSON and extract purposes dynamically
        ComplexConsentRule rule;
        List<String> consentedPurposes = new java.util.ArrayList<>();

        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId); // Override with test athlete ID

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
            row.put("athlete_id", athleteId);
            restTemplate.postForEntity("/api/ingest", row, Map.class);
        }

        // 3. Await Processing and Verify All Layers
        await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
            // Check Bronze
            boolean bronzeExists = checkBucketPathExists("bronze", athleteId);
            assertTrue(bronzeExists, "Bronze bucket should have data for athlete");

            // Check Silver
            boolean silverExists = checkBucketPathExists("silver", athleteId);
            assertTrue(silverExists, "Silver bucket should have data for athlete");

            // --- DYNAMIC Gold Layer Verification ---
            System.out.println("\n🔍 VERIFYING GOLD LAYER:");

            // Verify ALL consented purposes have data
            for (String purpose : consentedPurposes) {
                boolean exists = checkGoldPathExists(purpose, athleteId);
                assertTrue(exists, "Purpose '" + purpose + "' should have data in Gold layer");
                System.out.println("  ✅ Purpose '" + purpose + "': Data found");
            }

            // Verify unconsented purpose has NO data
            boolean marketingExists = checkGoldPathExists("marketing", athleteId);
            assertTrue(!marketingExists, "Marketing (unconsented) should have NO data");
            System.out.println("  ✅ Purpose 'marketing' (unconsented): No data");
        });
    }

    private boolean checkGoldPathExists(String purpose, String athleteId) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("gold").build())) {
            return false;
        }

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket("gold").recursive(true).build());

        for (Result<Item> result : results) {
            String name = result.get().objectName();
            if (name.contains("purpose/" + purpose) && name.contains("athlete_id=" + athleteId)) {
                return true;
            }
        }
        return false;
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
