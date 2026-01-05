package org.example.manual;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.config.KafkaConfig;
import org.example.consent.model.ComplexConsentRule;
import org.example.consent.model.ConsentChangedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DataFlowRevocationManualTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(DataFlowRevocationManualTest.class);

    @Test
    public void testRevocationCycle_Manual() throws Exception {
        String athleteId = "athlete-manual-rev-" + UUID.randomUUID().toString().substring(0, 8);
        String consentKey = "consent:rule:" + athleteId;

        System.out.println("\n" + "=".repeat(80));
        System.out.println("🔄 MANUAL REVOCATION TEST");
        System.out.println("=".repeat(80));
        System.out.println("📌 Make sure docker-compose is running!");
        System.out.println("🌐 Open MinIO Browser: http://localhost:9001");
        System.out.println("🆔 Athlete ID: " + athleteId);

        // STEP 1: GRANT & INGEST
        System.out.println("\n" + "-".repeat(80));
        System.out.println("STEP 1: Grant & Ingest");
        System.out.println("-".repeat(80));
        System.out.println("Loading Consent Rule from consent_rule.json...");

        ComplexConsentRule rule;
        String testPurpose; // Will use first consented purpose for revocation test
        int recordCount;

        try (InputStream stream = resourceLoader.getResource("classpath:consent_rule.json").getInputStream()) {
            List<ComplexConsentRule> rules = objectMapper.readValue(stream, new TypeReference<>() {
            });
            rule = rules.get(0); // Use first consent rule as template
            rule.setUserId(athleteId);

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

        System.out.println("✅ Consent Rule Created: " + testPurpose + " = ACTIVE");

        // Load and ingest data from JSON
        System.out.println("Loading test data from raw_data.json...");
        List<Map<String, Object>> dataRows;
        try (InputStream stream = resourceLoader.getResource("classpath:raw_data.json").getInputStream()) {
            dataRows = objectMapper.readValue(stream, new TypeReference<>() {
            });
        }

        recordCount = dataRows.size();
        System.out.println("📤 Ingesting " + recordCount + " data rows...");
        for (Map<String, Object> row : dataRows) {
            row.put("athlete_id", athleteId);
            restTemplate.postForEntity("/api/ingest", row, Map.class);
        }

        logger.info("\n" + "=".repeat(80));
        logger.info("⏸️  BREAKPOINT 1: Data Ingested");
        logger.info("=".repeat(80));
        logger.info(">> CHECK MINIO: Verify 'gold/active' contains files for athlete: {}", athleteId);
        logger.info("   Look for paths like: active/.../purpose/{}/athlete_id={}/", testPurpose, athleteId);
        logger.info("   Expected: {} files for purpose '{}'", recordCount, testPurpose);
        logger.info("   Set breakpoint here and inspect MinIO at http://localhost:9001");
        logger.info("=".repeat(80));
        // Breakpoint location - check MinIO before continuing

        // STEP 2: REVOKE
        System.out.println("\n" + "-".repeat(80));
        System.out.println("STEP 2: Revoke Consent");
        System.out.println("-".repeat(80));
        System.out.println("Updating Rule to INACTIVE and Publishing Revoke Event...");

        rule.setStatus("INACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent revokeEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.REVOKED, athleteId,
                testPurpose);
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, revokeEvent);

        logger.info("\n" + "=".repeat(80));
        logger.info("⏸️  BREAKPOINT 2: Revoke Event Sent");
        logger.info("=".repeat(80));
        logger.info(">> CHECK MINIO: Verify 'gold/active' is EMPTY for purpose '{}'", testPurpose);
        logger.info("   Verify 'gold/history/revoked_*' has data for athlete: {}", athleteId);
        logger.info("   Expected: {} files moved to history", recordCount);
        logger.info("   Set breakpoint here and inspect MinIO at http://localhost:9001");
        logger.info("=".repeat(80));
        // Breakpoint location - check MinIO after revocation

        // STEP 3: RE-GRANT (REPLAY)
        System.out.println("\n" + "-".repeat(80));
        System.out.println("STEP 3: Re-Grant (Replay)");
        System.out.println("-".repeat(80));
        System.out.println("Updating Rule to ACTIVE and Publishing Grant Event...");

        rule.setStatus("ACTIVE");
        redisTemplate.opsForValue().set(consentKey, objectMapper.writeValueAsString(rule));

        ConsentChangedEvent grantEvent = new ConsentChangedEvent(ConsentChangedEvent.Type.GRANTED, athleteId,
                testPurpose);
        kafkaTemplate.send(KafkaConfig.CONSENT_EVENTS, grantEvent);

        logger.info("\n" + "=".repeat(80));
        logger.info("⏸️  BREAKPOINT 3: Grant Event Sent (Replay)");
        logger.info("=".repeat(80));
        logger.info(">> CHECK MINIO: Verify 'gold/active' is POPULATED again with replayed data");
        logger.info("   Data should be replayed from Silver layer back to Gold active zone");
        logger.info("   Expected: {} files for purpose '{}'", recordCount, testPurpose);
        logger.info("   Set breakpoint here and inspect MinIO at http://localhost:9001");
        logger.info("=".repeat(80));
        logger.info("\n✅ Test Complete - Revocation cycle tested for purpose: {}", testPurpose);
        // Breakpoint location - check MinIO after re-grant
    }
}
