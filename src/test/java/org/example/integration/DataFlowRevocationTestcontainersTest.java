package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.messages.Item;
import org.example.config.KafkaConfig;
import org.example.consent.model.ComplexConsentRule;
import org.example.consent.model.ConsentChangedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.awaitility.Awaitility;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = "gold.index.manager.group-id=revocation-test-group")
public class DataFlowRevocationTestcontainersTest extends BaseTestcontainersTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

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
    void testRevocationCycle() throws Exception {
        String athleteId = "athlete-rev-" + UUID.randomUUID().toString().substring(0, 8);
        String consentKey = "consent:rule:" + athleteId;

        // 1. SETUP CONSENT RULE FROM JSON (load array and use first rule)
        ComplexConsentRule rule;
        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId); // Override with test athlete ID
            redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));
        }

        // 2. INGEST DATA FROM JSON
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

        for (Map<String, Object> row : dataRows) {
            row.put("athlete_id", athleteId);
            restTemplate.postForEntity("/api/ingest", row, Map.class);
        }

        // 3. VERIFY DATA IN ACTIVE ZONE (Wait for processing)
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Iterable<Result<Item>> items = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket("gold").prefix("active/").recursive(true).build());
            for (Result<Item> item : items) {
                if (item.get().objectName().contains("purpose/research") && item.get().objectName().contains(athleteId))
                    return true;
            }
            return false;
        });

        System.out.println("STEP 1: Data found in Active Zone.");

        // 4. REVOKE CONSENT FOR RESEARCH
        rule.setStatus("INACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent revokeEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.REVOKED, athleteId,
                "research");
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, revokeEvent);

        // 5. VERIFY DATA MOVED TO HISTORY
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            // Check Active is Empty for research
            Iterable<Result<Item>> activeItems = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket("gold").prefix("active/").recursive(true).build());
            for (Result<Item> item : activeItems) {
                if (item.get().objectName().contains("purpose/research") && item.get().objectName().contains(athleteId))
                    return false;
            }

            // Check History has the data
            Iterable<Result<Item>> historyItems = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket("gold").prefix("history/").recursive(true).build());
            for (Result<Item> item : historyItems) {
                if (item.get().objectName().contains("purpose/research") && item.get().objectName().contains(athleteId))
                    return true;
            }
            return false;
        });

        System.out.println("STEP 2: Data moved to History Zone.");

        // 6. RE-GRANT CONSENT (Replay from Silver)
        rule.setStatus("ACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent grantEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.GRANTED, athleteId,
                "research");
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, grantEvent);

        // 7. VERIFY DATA REPLAYED BACK TO ACTIVE
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Iterable<Result<Item>> items = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket("gold").prefix("active/").recursive(true).build());
            for (Result<Item> item : items) {
                if (item.get().objectName().contains("purpose/research") && item.get().objectName().contains(athleteId))
                    return true;
            }
            return false;
        });

        System.out.println("STEP 3: Data replayed back to Active Zone.");
    }
}
