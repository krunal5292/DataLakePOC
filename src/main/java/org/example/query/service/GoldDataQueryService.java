package org.example.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.exception.ConsentViolationException;
import org.example.query.model.DataItem;
import org.example.query.model.GoldDataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GoldDataQueryService {

    private static final Logger log = LoggerFactory.getLogger(GoldDataQueryService.class);

    private final MinioClient minioClient;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${minio.bucket.gold:gold}")
    private String goldBucket;

    public GoldDataQueryService(MinioClient minioClient, StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.minioClient = minioClient;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Query athlete data for specific purposes with consent validation
     */
    public GoldDataResponse queryAthleteData(String athleteId, List<String> requestedPurposes) throws Exception {
        log.info("Querying data for athlete: {} with purposes: {}", athleteId, requestedPurposes);

        // Get approved purposes from consent
        List<String> approvedPurposes = getApprovedPurposes(athleteId);

        // Validate all requested purposes are consented
        for (String purpose : requestedPurposes) {
            if (!approvedPurposes.contains(purpose)) {
                log.warn("Consent violation: Athlete {} has not consented to purpose: {}", athleteId, purpose);
                throw new ConsentViolationException(athleteId, purpose);
            }
        }

        // Fetch data only for approved purposes
        List<DataItem> dataItems = fetchDataForPurposes(athleteId, requestedPurposes);

        log.info("Retrieved {} records for athlete {} across {} purposes",
                dataItems.size(), athleteId, requestedPurposes.size());

        return new GoldDataResponse(athleteId, requestedPurposes, dataItems);
    }

    /**
     * Get ALL data the athlete has consented to
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
     * Validate if athlete has consented to a specific purpose
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

        ComplexConsentRule rule = objectMapper.readValue(consentJson, ComplexConsentRule.class);

        if (!"ACTIVE".equals(rule.getStatus())) {
            log.warn("Consent rule is not ACTIVE for athlete: {}", athleteId);
            return new ArrayList<>();
        }

        // Extract approved purposes from consent rule
        if (rule.getDimensions() != null && rule.getDimensions().getPurpose() != null) {
            return rule.getDimensions().getPurpose().getValues().stream()
                    .map(ComplexConsentRule.ValueDetail::getValue)
                    .collect(Collectors.toList());
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

                // Filter by purpose and athlete_id
                if (objectPath.contains("/purpose/" + purpose + "/") &&
                        objectPath.contains("/athlete_id=" + athleteId + "/")) {

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
