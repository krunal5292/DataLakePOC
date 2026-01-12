package org.example.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.example.query.model.BuyerDataItem;
import org.example.query.model.BuyerDataResponse;
import org.example.query.strategy.BuyerQueryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Partition-based implementation of BuyerQueryStrategy.
 * Queries data from MinIO gold bucket using partition paths.
 * 
 * IMPORTANT: This implementation does NOT check consent at query time.
 * It relies on the "Physical Storage as Source of Truth" principle.
 * Data is pre-indexed during ingestion based on consent rules.
 */
@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "partition", matchIfMissing = true)
public class PartitionBuyerQueryStrategy implements BuyerQueryStrategy {

    private static final Logger log = LoggerFactory.getLogger(PartitionBuyerQueryStrategy.class);

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    public PartitionBuyerQueryStrategy(MinioClient minioClient, ObjectMapper objectMapper) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public BuyerDataResponse fetchByPurpose(String purpose) {
        log.info("Fetching data for purpose: {} using partition strategy", purpose);

        List<BuyerDataItem> dataItems = new ArrayList<>();

        try {
            // Path pattern: active/*/purpose/{purpose}/athlete_id={athleteId}/*
            String prefix = "active/";

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(goldBucket)
                            .prefix(prefix)
                            .recursive(true)
                            .build());

            for (Result<Item> result : results) {
                Item item = result.get();
                String objectPath = item.objectName();

                // Filter by purpose in path
                if (objectPath.contains("/purpose/" + purpose + "/")) {
                    BuyerDataItem dataItem = extractDataItem(objectPath);
                    if (dataItem != null) {
                        dataItems.add(dataItem);
                    }
                }
            }

            log.info("Retrieved {} records for purpose: {}", dataItems.size(), purpose);

        } catch (Exception e) {
            log.error("Error fetching data for purpose: {}", purpose, e);
            throw new RuntimeException("Failed to fetch data for purpose: " + purpose, e);
        }

        return new BuyerDataResponse(purpose, dataItems);
    }

    @Override
    public BuyerDataResponse fetchByPurposes(List<String> purposes) {
        log.info("Fetching data for {} purposes using partition strategy", purposes.size());

        List<BuyerDataItem> allDataItems = new ArrayList<>();

        for (String purpose : purposes) {
            BuyerDataResponse response = fetchByPurpose(purpose);
            allDataItems.addAll(response.getData());
        }

        BuyerDataResponse combinedResponse = new BuyerDataResponse();
        combinedResponse.setPurpose(String.join(",", purposes));
        combinedResponse.setData(allDataItems);

        return combinedResponse;
    }

    @Override
    public String getStrategyName() {
        return "partition";
    }

    /**
     * Extract data item from MinIO object.
     * Path format:
     * active/YYYY-MM-DD/purpose/{PURPOSE}/athlete_id={ID}/data_TRACEID.json
     */
    private BuyerDataItem extractDataItem(String objectPath) {
        try {
            // Fetch object content
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(goldBucket)
                            .object(objectPath)
                            .build())) {

                @SuppressWarnings("unchecked")
                Map<String, Object> content = objectMapper.readValue(stream, Map.class);

                // Extract metadata from content
                String athleteId = (String) content.get("athlete_id");
                String eventId = (String) content.get("event_id");
                String activityType = (String) content.get("activity_type");

                // Extract timestamp from path (e.g., active/2026-01-12/...)
                String timestamp = extractTimestampFromPath(objectPath);

                return new BuyerDataItem(athleteId, eventId, activityType, timestamp, content);
            }
        } catch (Exception e) {
            log.error("Error extracting data from path: {}", objectPath, e);
            return null;
        }
    }

    /**
     * Extract timestamp from object path.
     */
    private String extractTimestampFromPath(String path) {
        // Path format: active/2026-01-12/purpose/...
        String[] parts = path.split("/");
        if (parts.length > 1) {
            return parts[1]; // Return the date part
        }
        return "unknown";
    }
}
