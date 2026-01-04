package org.example.etl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.*;
import io.minio.messages.Item;
import org.example.consent.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Service
public class EtlService {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final ClickHouseService clickHouseService;

    @Value("${minio.bucket.bronze}")
    private String bronzeBucket;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    public EtlService(MinioClient minioClient, ObjectMapper objectMapper, StringRedisTemplate redisTemplate, ClickHouseService clickHouseService) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
        this.clickHouseService = clickHouseService;
    }

    // @Scheduled(fixedDelay = 30000) // Every 30 seconds
    public void processBronzeToSilver() {
        try {
            // Ensure Bronze bucket exists (it should be created by IngestionService, but let's be safe)
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bronzeBucket).build())) {
                return;
            }

            // Ensure Silver bucket exists
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(silverBucket).build())) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(silverBucket).build());
            }

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder().bucket(bronzeBucket).prefix("pending/").recursive(true).build()
            );

            for (Result<Item> result : results) {
                Item item = result.get();
                processSingleBronzeFile(item.objectName());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processSingleBronzeFile(String objectName) throws Exception {
        // Read JSON
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder().bucket(bronzeBucket).object(objectName).build())) {
            
            Map<String, Object> raw = objectMapper.readValue(stream, Map.class);
            
            // Transform to Silver (Structured)
            Map<String, Object> silverData = new HashMap<>();
            String athleteId = (String) raw.getOrDefault("athlete_id", "unknown");
            
            silverData.put("athlete_id", athleteId);
            silverData.put("ts", raw.getOrDefault("ts", System.currentTimeMillis()));
            
            if (raw.get("data") instanceof Map) {
                Map<String, Object> data = (Map<String, Object>) raw.get("data");
                silverData.put("activity_id", data.get("activity_id"));
                silverData.put("heart_rate", data.get("heart_rate"));
                silverData.put("blood_pressure_sys", data.get("blood_pressure_sys"));
                silverData.put("oxygen_sat", data.get("oxygen_sat"));
            }
            
            // Write to Silver (pending/)
            String silverPath = "pending/" + objectName.replace("pending/", ""); 
            String silverJson = objectMapper.writeValueAsString(silverData);
            byte[] bytes = silverJson.getBytes(StandardCharsets.UTF_8);
            
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(silverBucket)
                            .object(silverPath)
                            .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                            .contentType("application/json")
                            .build()
            );
            
            // Move Bronze file to 'processed'
            String processedPath = objectName.replace("pending/", "processed/");
            minioClient.copyObject(
                    CopyObjectArgs.builder()
                            .bucket(bronzeBucket)
                            .object(processedPath)
                            .source(CopySource.builder().bucket(bronzeBucket).object(objectName).build())
                            .metadataDirective(io.minio.Directive.REPLACE)
                            .headers(java.util.Map.of("Content-Type", "application/json"))
                            .build()
            );
            
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(bronzeBucket).object(objectName).build()
            );
            
            System.out.println("Processed Bronze -> Silver: " + objectName);
        }
    }

    // @Scheduled(fixedDelay = 60000) // Every 1 minute
    public void processSilverToGold() {
        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(silverBucket).build())) {
                return;
            }

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder().bucket(silverBucket).prefix("pending/").recursive(true).build()
            );

            for (Result<Item> result : results) {
                Item item = result.get();
                processSingleSilverFile(item.objectName());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processSingleSilverFile(String objectName) throws Exception {
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder().bucket(silverBucket).object(objectName).build())) {
            
            Map<String, Object> data = objectMapper.readValue(stream, Map.class);
            String athleteId = (String) data.get("athlete_id");
            
            // 1. Fetch Consent Mask
            String maskStr = redisTemplate.opsForValue().get("consent:" + athleteId);
            long mask = maskStr != null ? Long.parseLong(maskStr) : 0L;
            
            // Handle Timestamp (millis to seconds)
            Object tsObj = data.get("ts");
            long ts = 0;
            if (tsObj instanceof Number) {
                ts = ((Number) tsObj).longValue();
                if (ts > 100000000000L) { // Heuristic for millis (e.g., year 2000+)
                    ts = ts / 1000;
                }
            }

            // 2. Insert into ClickHouse
            // Schema: athlete_id, activity_id, heart_rate, blood_pressure_sys, oxygen_sat, ts, consent_version, consent_mask
            String sql = "INSERT INTO athlete_data_gold (athlete_id, activity_id, heart_rate, blood_pressure_sys, oxygen_sat, ts, consent_mask) VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            clickHouseService.executeUpdate(sql, 
                athleteId,
                data.get("activity_id"),
                data.get("heart_rate"),
                data.get("blood_pressure_sys"),
                data.get("oxygen_sat"),
                ts, 
                mask
            );
            
            // 3. Move Silver to processed
            String processedPath = objectName.replace("pending/", "processed/");
            minioClient.copyObject(
                    CopyObjectArgs.builder()
                            .bucket(silverBucket)
                            .object(processedPath)
                            .source(CopySource.builder().bucket(silverBucket).object(objectName).build())
                            .metadataDirective(io.minio.Directive.REPLACE)
                            .headers(java.util.Map.of("Content-Type", "application/json"))
                            .build()
            );
            
            minioClient.removeObject(
                    RemoveObjectArgs.builder().bucket(silverBucket).object(objectName).build()
            );
            
            System.out.println("Processed Silver -> Gold: " + objectName);
        }
    }
}
