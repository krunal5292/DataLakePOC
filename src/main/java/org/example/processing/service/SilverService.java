package org.example.processing.service;

import io.minio.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Map;

@Service
public class SilverService {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final MetadataService metadataService;

    @Value("${minio.bucket.bronze}")
    private String bronzeBucket;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    public SilverService(MinioClient minioClient, ObjectMapper objectMapper, MetadataService metadataService) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.metadataService = metadataService;
    }

    public String processAndSave(TelemetryMessage message) {
        try {
            // 1. Read from Bronze
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(bronzeBucket).object(message.getMinioPath()).build())) {
                
                Map<String, Object> rawData = objectMapper.readValue(stream, Map.class);
                
                // 2. Enrich
                rawData.put("athlete_name", metadataService.getAthleteName(message.getAthleteId()));
                String eventId = (String) rawData.get("event_id");
                rawData.put("event_name", metadataService.getEventName(eventId));
                rawData.put("enriched_at", System.currentTimeMillis());
                
                // 3. Save to Silver
                LocalDate now = LocalDate.now();
                String silverPath = String.format("pending/%s/%s/%s.json", now.toString(), message.getAthleteId(), message.getTraceId());
                
                byte[] bytes = objectMapper.writeValueAsString(rawData).getBytes(StandardCharsets.UTF_8);
                
                if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(silverBucket).build())) {
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(silverBucket).build());
                }

                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(silverBucket)
                                .object(silverPath)
                                .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                                .contentType("application/json")
                                .build()
                );
                
                return silverPath;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to process Silver layer", e);
        }
    }
}
