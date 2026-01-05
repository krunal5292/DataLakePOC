package org.example.processing.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.messages.Item;
import org.example.consent.model.ComplexConsentRule;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Map;

/**
 * Physical Partitioning strategy for Gold layer consent enforcement.
 * This strategy physically duplicates data into separate folders per purpose,
 * enabling zero-latency reads through partition pruning.
 */
@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "partition", matchIfMissing = true)
public class PhysicalPartitionStrategy implements GoldEnforcementStrategy {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    public PhysicalPartitionStrategy(MinioClient minioClient, ObjectMapper objectMapper,
            StringRedisTemplate redisTemplate) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void processAndFanOut(TelemetryMessage message) {
        try {
            // 1. Read Enriched Silver Data
            Map<String, Object> enrichedData;
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(silverBucket).object(message.getMinioPath()).build())) {
                enrichedData = objectMapper.readValue(stream, Map.class);
            }

            // 2. Fetch Consent Rule from Redis
            String ruleJson = redisTemplate.opsForValue().get("consent:rule:" + message.getAthleteId());
            if (ruleJson == null) {
                System.out.println("No consent rule found for athlete: " + message.getAthleteId());
                return;
            }
            ComplexConsentRule rule = objectMapper.readValue(ruleJson, ComplexConsentRule.class);

            if (!"ACTIVE".equals(rule.getStatus()))
                return;

            // 3. Evaluate Event Match
            String eventId = (String) enrichedData.get("event_id");
            boolean eventAllowed = isValueAllowed(rule.getDimensions().getEvents(), eventId);
            if (!eventAllowed)
                return;

            // 4. Fan-Out per Purpose
            ComplexConsentRule.DimensionDetail purposeDim = rule.getDimensions().getPurpose();
            if ("any".equals(purposeDim.getType())) {
                // Handle 'any' if needed, for POC we focus on specific list
            } else {
                for (ComplexConsentRule.ValueDetail purpose : purposeDim.getValues()) {
                    writeToGold(purpose.getValue(), message.getAthleteId(), message.getTraceId(), enrichedData);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to process Gold layer", e);
        }
    }

    @Override
    public void handleRevocation(String athleteId, String purpose) {
        try {
            // Move data from active/ to history/
            String activePrefix = String.format("active/");
            String historyPrefix = String.format("history/%s/", LocalDate.now());

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(goldBucket)
                            .prefix(activePrefix)
                            .recursive(true)
                            .build());

            for (Result<Item> result : results) {
                String objectName = result.get().objectName();

                // Check if this object matches the athlete and purpose
                if (objectName.contains("purpose/" + purpose) && objectName.contains("athlete_id=" + athleteId)) {
                    // Copy to history
                    String newPath = objectName.replace("active/", historyPrefix);

                    minioClient.copyObject(
                            CopyObjectArgs.builder()
                                    .bucket(goldBucket)
                                    .object(newPath)
                                    .source(CopySource.builder()
                                            .bucket(goldBucket)
                                            .object(objectName)
                                            .build())
                                    .build());

                    // Delete from active
                    minioClient.removeObject(
                            RemoveObjectArgs.builder()
                                    .bucket(goldBucket)
                                    .object(objectName)
                                    .build());

                    System.out.println("Moved to history: " + objectName + " -> " + newPath);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to handle revocation", e);
        }
    }

    @Override
    public void handleGrant(String athleteId, String purpose) {
        // TODO: Implement backfill from Silver layer
        // This would:
        // 1. Query Silver bucket for all data for this athlete
        // 2. Re-run processAndFanOut for each record
        // 3. Only write to the newly granted purpose
        System.out.println("Grant handler not yet implemented for: " + athleteId + ", purpose: " + purpose);
    }

    @Override
    public boolean verifyDataExists(String athleteId, String purpose) {
        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(goldBucket).build())) {
                return false;
            }

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(goldBucket)
                            .prefix("active/")
                            .recursive(true)
                            .build());

            for (Result<Item> result : results) {
                String name = result.get().objectName();
                if (name.contains("purpose/" + purpose) && name.contains("athlete_id=" + athleteId)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify data existence", e);
        }
    }

    @Override
    public String getStrategyName() {
        return "partition";
    }

    private boolean isValueAllowed(ComplexConsentRule.DimensionDetail dim, String value) {
        if ("any".equals(dim.getType()))
            return true;
        if (value == null)
            return false;
        return dim.getValues().stream().anyMatch(v -> value.equals(v.getValue()));
    }

    private void writeToGold(String purpose, String athleteId, String traceId, Map<String, Object> data)
            throws Exception {
        LocalDate now = LocalDate.now();
        // Path: active/YYYY-MM-DD/purpose/PURPOSE/athlete_id=ID/data_TRACEID.json
        String goldPath = String.format("active/%s/purpose/%s/athlete_id=%s/data_%s.json",
                now.toString(), purpose, athleteId, traceId);

        byte[] bytes = objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);

        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(goldBucket).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(goldBucket).build());
        }

        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(goldBucket)
                        .object(goldPath)
                        .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                        .contentType("application/json")
                        .build());
        System.out.println("Written to Gold: " + goldPath);
    }
}
