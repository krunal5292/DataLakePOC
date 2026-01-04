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

        // 1. Setup Complex Consent Rule from JSON (load array and use first rule)
        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            ComplexConsentRule rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId); // Override with test athlete ID
            redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));
        }

        // 2. Ingest Data from JSON
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

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

            // Check Gold
            boolean researchExists = checkGoldPathExists("research", athleteId);
            boolean medicalExists = checkGoldPathExists("healthAndMedical", athleteId);

            assertTrue(researchExists, "Research bucket should have data");
            assertTrue(medicalExists, "Medical bucket should have data");
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
