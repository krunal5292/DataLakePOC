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

    @Autowired
    private org.example.processing.service.GoldService goldService;

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

        // 1. SETUP CONSENT RULE FROM JSON and extract first purpose dynamically
        ComplexConsentRule rule;
        String testPurpose; // Will use first consented purpose for revocation test

        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId); // Override with test athlete ID

            // Extract first consented purpose dynamically
            if (rule.getDimensions().getPurpose() != null
                    && rule.getDimensions().getPurpose().getValues() != null
                    && !rule.getDimensions().getPurpose().getValues().isEmpty()) {
                testPurpose = rule.getDimensions().getPurpose().getValues().get(0).getValue();
            } else {
                throw new IllegalStateException("Consent rule must have at least one purpose");
            }

            redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));
        }

        System.out.println("\n📋 Testing revocation cycle for purpose: " + testPurpose);

        // 2. INGEST DATA FROM JSON
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

        System.out.println("📤 Ingesting " + dataRows.size() + " records...\n");

        for (Map<String, Object> row : dataRows) {
            row.put("athlete_id", athleteId);
            restTemplate.postForEntity("/api/ingest", row, Map.class);
        }

        // 3. VERIFY DATA IN ACTIVE ZONE (Wait for processing)
        String finalTestPurpose = testPurpose; // For lambda
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            return goldService.verifyDataExists(athleteId, finalTestPurpose);
        });

        System.out.println("✅ STEP 1: Data found in Active Zone for purpose '" + testPurpose + "'");

        // 4. REVOKE CONSENT FOR THE TEST PURPOSE
        rule.setStatus("INACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent revokeEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.REVOKED, athleteId,
                testPurpose);
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, revokeEvent);

        // 5. VERIFY DATA MOVED TO HISTORY (Data no longer accessible via strategy)
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            // Strategy should report data doesn't exist (moved to history)
            return !goldService.verifyDataExists(athleteId, finalTestPurpose);
        });

        System.out.println("✅ STEP 2: Data moved to History Zone for purpose '" + testPurpose + "'");

        // 6. RE-GRANT CONSENT (Replay from Silver)
        rule.setStatus("ACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent grantEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.GRANTED, athleteId,
                testPurpose);
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, grantEvent);

        // 7. VERIFY DATA REPLAYED BACK TO ACTIVE
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            return goldService.verifyDataExists(athleteId, finalTestPurpose);
        });

        System.out.println("✅ STEP 3: Data replayed back to Active Zone for purpose '" + testPurpose + "'");
        System.out.println("\n🎯 Revocation cycle test complete!");
    }
}
