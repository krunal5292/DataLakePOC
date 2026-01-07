package org.example.processing.service;

import io.minio.*;
import io.minio.messages.Item;
import org.example.config.KafkaConfig;
import org.example.consent.model.ConsentChangedEvent;
import org.example.ingestion.model.TelemetryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class GoldIndexManager {

    private static final Logger log = LoggerFactory.getLogger(GoldIndexManager.class);

    private final MinioClient minioClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    public GoldIndexManager(MinioClient minioClient, KafkaTemplate<String, Object> kafkaTemplate) {
        this.minioClient = minioClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void handleEvent(ConsentChangedEvent event) {
        log.info("Handling Consent Event: {}", event);
        try {
            if (event.getType() == ConsentChangedEvent.Type.REVOKED) {
                moveToHistory(event.getAthleteId(), event.getPurpose());
            } else if (event.getType() == ConsentChangedEvent.Type.GRANTED) {
                replayFromSilver(event.getAthleteId());
            }
        } catch (Exception e) {
            log.error("Failed to handle consent event", e);
            throw new RuntimeException(e);
        }
    }

    // SCENARIO 1: REVOCATION (Move to History)
    private void moveToHistory(String athleteId, String purpose) throws Exception {
        String activePrefix = String.format("active/"); // We scan all dates
        // Pattern logic: active/*/purpose/{purpose}/athlete_id={athleteId}/...

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(goldBucket)
                        .prefix(activePrefix)
                        .recursive(true)
                        .build());

        String targetSubstring = String.format("/purpose/%s/athlete_id=%s/", purpose, athleteId);
        String timestamp = LocalDate.now().toString(); // Use date for simple history partitioning

        int count = 0;
        for (Result<Item> result : results) {
            Item item = result.get();
            String sourcePath = item.objectName();
            count++;

            // System.out.println("DEBUG: Check file: " + sourcePath);

            if (sourcePath.contains(targetSubstring)) {
                System.out.println("DEBUG: MATCH FOUND! " + sourcePath);
                // Construct History Path
                // source: active/2025-12-31/purpose/research/athlete_id=123/full.json
                // dest:
                // history/revoked_2026-01-01/2025-12-31/purpose/research/athlete_id=123/full.json
                String destPath = sourcePath.replace("active/", "history/revoked_" + timestamp + "/");

                // Copy
                minioClient.copyObject(
                        CopyObjectArgs.builder()
                                .bucket(goldBucket)
                                .object(destPath)
                                .source(CopySource.builder().bucket(goldBucket).object(sourcePath).build())
                                .build());

                // Delete
                minioClient.removeObject(
                        RemoveObjectArgs.builder().bucket(goldBucket).object(sourcePath).build());

                log.info("Moved to History: {} -> {}", sourcePath, destPath);
            }
        }
    }

    // SCENARIO 2: ADDITION (Replay from Silver)
    private void replayFromSilver(String athleteId) throws Exception {
        // Scan Silver for this athlete
        // Path: silver/pending/YYYY-MM-DD/athlete_id={id}/...

        // Note: In a real system, we might need a more optimized index for Silver.
        // For POC, we scan the silver bucket recursively looking for athlete_id

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(silverBucket)
                        .recursive(true)
                        .build());

        String targetStr = String.format("/%s/", athleteId);

        for (Result<Item> result : results) {
            Item item = result.get();
            if (item.objectName().contains(targetStr)) {
                // Found a silver file for this athlete. Trigger Replay.
                // We need to reconstruct a basic TelemetryMessage to trigger the GoldProcessor.
                // The GoldProcessor needs: traceId (from filename), athleteId (known),
                // minioPath (known)

                String path = item.objectName();
                String filename = path.substring(path.lastIndexOf('/') + 1); // traceId.json
                String traceId = filename.replace(".json", "");

                TelemetryMessage replayMsg = new TelemetryMessage();
                replayMsg.setAthleteId(athleteId);
                replayMsg.setTraceId(traceId);
                replayMsg.setMinioPath(path);

                // Publish to the existing Silver-Saved topic to trigger Gold Processor
                kafkaTemplate.send(KafkaConfig.TELEMETRY_SILVER_SAVED, traceId, replayMsg);
                log.info("Triggered Replay for Trace: {}", traceId);
            }
        }
    }
}
