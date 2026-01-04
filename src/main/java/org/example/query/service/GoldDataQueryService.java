package org.example.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.example.query.model.DataItem;
import org.example.query.model.GoldDataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class GoldDataQueryService {

    private static final Logger log = LoggerFactory.getLogger(GoldDataQueryService.class);

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    public GoldDataQueryService(MinioClient minioClient, ObjectMapper objectMapper,
            org.springframework.data.redis.core.StringRedisTemplate redisTemplate) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    /**
     * Query athlete data for specific purposes.
     * <p>
     * ARCHITECTURE NOTE:
     * This method does NOT check Redis for consent.
     * It relies on the "Physical Storage as Source of Truth" principle.
     * If data exists in 'gold/active/purpose=X', it is implicitly consented.
     * Unconsented data would have been filtered out by the Write Path (Silver ->
     * Gold).
     */
    public GoldDataResponse queryAthleteData(String athleteId, List<String> requestedPurposes) throws Exception {
        log.info("Querying data for athlete: {} with purposes: {}", athleteId, requestedPurposes);

        // Fetch data directly from the physical storage
        List<DataItem> dataItems = fetchDataForPurposes(athleteId, requestedPurposes);

        log.info("Retrieved {} records for athlete {} across {} purposes",
                dataItems.size(), athleteId, requestedPurposes.size());

        return new GoldDataResponse(athleteId, requestedPurposes, dataItems);
    }

    /**
     * Get ALL data the athlete has consented to (LEGACY METHOD FOR MANUAL TEST)
     */
    public GoldDataResponse getAllConsentedData(String athleteId) throws Exception {
        log.info("Querying ALL consented data for athlete: {}", athleteId);

        // Get all approved purposes from consent
        List<String> approvedPurposes = getApprovedPurposes(athleteId);

        if (approvedPurposes.isEmpty()) {
            log.warn("No consent found for athlete: {}", athleteId);
            return new GoldDataResponse(athleteId, approvedPurposes, new ArrayList<>());
        }

        // Fetch data for all approved purposes
        List<DataItem> dataItems = fetchDataForPurposes(athleteId, approvedPurposes);

        log.info("Retrieved {} total records for athlete {} across {} consented purposes",
                dataItems.size(), athleteId, approvedPurposes.size());

        return new GoldDataResponse(athleteId, approvedPurposes, dataItems);
    }

    /**
     * Validate if athlete has consented to a specific purpose (LEGACY METHOD FOR
     * MANUAL TEST)
     */
    public boolean validateConsent(String athleteId, String purpose) {
        try {
            List<String> approvedPurposes = getApprovedPurposes(athleteId);
            return approvedPurposes.contains(purpose);
        } catch (Exception e) {
            log.error("Error validating consent for athlete: {}, purpose: {}", athleteId, purpose, e);
            return false;
        }
    }

    /**
     * Get list of purposes the athlete has consented to
     */
    public List<String> getApprovedPurposes(String athleteId) throws Exception {
        String consentKey = "consent:rule:" + athleteId;
        String consentJson = redisTemplate.opsForValue().get(consentKey);

        if (consentJson == null) {
            log.warn("No consent rule found for athlete: {}", athleteId);
            return new ArrayList<>();
        }

        org.example.consent.model.ComplexConsentRule rule = objectMapper.readValue(consentJson,
                org.example.consent.model.ComplexConsentRule.class);

        // Extract approved purposes from consent rule
        if (rule.getDimensions() != null && rule.getDimensions().getPurpose() != null) {
            return rule.getDimensions().getPurpose().getValues().stream()
                    .map(org.example.consent.model.ComplexConsentRule.ValueDetail::getValue)
                    .collect(java.util.stream.Collectors.toList());
        }

        return new ArrayList<>();
    }

    /**
     * Fetch data from Gold bucket for specific purposes
     */
    private List<DataItem> fetchDataForPurposes(String athleteId, List<String> purposes) throws Exception {
        List<DataItem> allData = new ArrayList<>();

        for (String purpose : purposes) {
            // Build path pattern: active/*/purpose/{purpose}/athlete_id={athleteId}/*
            // Note: We scan recursively from "active/" because date partitioning makes
            // direct lookup harder without a date range
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

                // Path format: active/YYYY-MM-DD/purpose/{PURPOSE}/athlete_id={ID}/...
                boolean purposeMatch = objectPath.contains("/purpose/" + purpose + "/");
                boolean athleteMatch = objectPath.contains("/athlete_id=" + athleteId + "/");

                if (purposeMatch && athleteMatch) {
                    // Fetch object content
                    try (InputStream stream = minioClient.getObject(
                            GetObjectArgs.builder()
                                    .bucket(goldBucket)
                                    .object(objectPath)
                                    .build())) {

                        @SuppressWarnings("unchecked")
                        Map<String, Object> content = objectMapper.readValue(stream, Map.class);

                        // Extract timestamp from path (e.g., active/2026-01-02/...)
                        String timestamp = extractTimestampFromPath(objectPath);

                        DataItem dataItem = new DataItem(purpose, objectPath, timestamp, content);
                        allData.add(dataItem);
                    }
                }
            }
        }

        return allData;
    }

    /**
     * Extract timestamp from object path
     */
    private String extractTimestampFromPath(String path) {
        // Path format: active/2026-01-02/purpose/...
        String[] parts = path.split("/");
        if (parts.length > 1) {
            return parts[1]; // Return the date part
        }
        return "unknown";
    }
}
