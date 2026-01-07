package org.example.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

@Service
public class IngestionService {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;

    @Value("${minio.bucket.bronze}")
    private String bronzeBucket;

    public IngestionService(MinioClient minioClient, ObjectMapper objectMapper) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
    }

    public String saveToBronze(TelemetryMessage message) {
        LocalDate now = LocalDate.now();
        // Path: pending/YYYY-MM-DD/athleteId/traceId.json
        String path = String.format("pending/%s/%s/%s.json", now.toString(), message.getAthleteId(), message.getTraceId());

        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bronzeBucket).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bronzeBucket).build());
            }

            String json = objectMapper.writeValueAsString(message.getPayload());
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bronzeBucket)
                            .object(path)
                            .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                            .contentType("application/json")
                            .build()
            );

            return path;
        } catch (Exception e) {
            throw new RuntimeException("Failed to save data to Bronze layer", e);
        }
    }
}