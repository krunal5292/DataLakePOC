package org.example.processing.service;

import io.minio.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.consent.model.ComplexConsentRule;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Service
public class GoldService {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    public GoldService(MinioClient minioClient, ObjectMapper objectMapper, StringRedisTemplate redisTemplate) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

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
