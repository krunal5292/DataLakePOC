package org.example.query.service;

import org.apache.iceberg.data.Record;
import org.example.query.model.BuyerDataItem;
import org.example.query.model.BuyerDataResponse;
import org.example.query.strategy.BuyerQueryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Iceberg-based implementation of BuyerQueryStrategy.
 * Queries data from Iceberg tables using IcebergQueryService.
 * 
 * IMPORTANT: This implementation does NOT check consent at query time.
 * It relies on the "Physical Storage as Source of Truth" principle.
 * Data is pre-indexed during ingestion based on consent rules.
 */
@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "iceberg")
public class IcebergBuyerQueryStrategy implements BuyerQueryStrategy {

    private static final Logger log = LoggerFactory.getLogger(IcebergBuyerQueryStrategy.class);

    private final IcebergQueryService icebergQueryService;

    public IcebergBuyerQueryStrategy(IcebergQueryService icebergQueryService) {
        this.icebergQueryService = icebergQueryService;
    }

    @Override
    public BuyerDataResponse fetchByPurpose(String purpose) {
        log.info("Fetching data for purpose: {} using Iceberg strategy", purpose);

        // Use IcebergQueryService to fetch records
        List<Record> records = icebergQueryService.fetchByPurpose(purpose);

        // Convert Iceberg Records to BuyerDataItems
        List<BuyerDataItem> dataItems = convertRecordsToDataItems(records);

        log.info("Retrieved {} records for purpose: {}", dataItems.size(), purpose);

        return new BuyerDataResponse(purpose, dataItems);
    }

    @Override
    public BuyerDataResponse fetchByPurposes(List<String> purposes) {
        log.info("Fetching data for {} purposes using Iceberg strategy", purposes.size());

        // Fetch records for each purpose and combine
        List<BuyerDataItem> allDataItems = new ArrayList<>();

        for (String purpose : purposes) {
            List<Record> records = icebergQueryService.fetchByPurpose(purpose);
            allDataItems.addAll(convertRecordsToDataItems(records));
        }

        BuyerDataResponse response = new BuyerDataResponse();
        response.setPurpose(String.join(",", purposes));
        response.setData(allDataItems);

        log.info("Retrieved {} total records for {} purposes", allDataItems.size(), purposes.size());

        return response;
    }

    @Override
    public String getStrategyName() {
        return "iceberg";
    }

    /**
     * Convert list of Iceberg Records to BuyerDataItems.
     */
    private List<BuyerDataItem> convertRecordsToDataItems(List<Record> records) {
        List<BuyerDataItem> dataItems = new ArrayList<>();

        for (Record record : records) {
            BuyerDataItem item = convertRecordToDataItem(record);
            dataItems.add(item);
        }

        return dataItems;
    }

    /**
     * Convert Iceberg Record to BuyerDataItem.
     */
    private BuyerDataItem convertRecordToDataItem(Record record) {
        String athleteId = (String) record.getField("athlete_id");
        String eventId = (String) record.getField("event_id");
        String activityType = (String) record.getField("activity_type");

        // Convert OffsetDateTime to String
        OffsetDateTime timestamp = (OffsetDateTime) record.getField("timestamp");
        String timestampStr = timestamp != null ? timestamp.toString() : null;

        // Extract data field (JSON string) and parse it
        String dataJson = (String) record.getField("data");
        Map<String, Object> content = new HashMap<>();

        if (dataJson != null) {
            try {
                // Parse the JSON data field
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> parsedData = mapper.readValue(dataJson, Map.class);
                content = parsedData;
            } catch (Exception e) {
                log.warn("Failed to parse data JSON for athlete: {}", athleteId, e);
                content.put("raw_data", dataJson);
            }
        }

        // Add metadata to content
        content.put("athlete_id", athleteId);
        content.put("event_id", eventId);
        content.put("activity_type", activityType);
        content.put("timestamp", timestampStr);

        return new BuyerDataItem(athleteId, eventId, activityType, timestampStr, content);
    }
}
