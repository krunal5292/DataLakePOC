package org.example.query.model;

import java.util.Map;

/**
 * Individual data item in buyer response.
 */
public class BuyerDataItem {

    private String athleteId;
    private String eventId;
    private String activityType;
    private String timestamp;
    private Map<String, Object> content;

    public BuyerDataItem() {
    }

    public BuyerDataItem(String athleteId, String eventId, String activityType, String timestamp,
            Map<String, Object> content) {
        this.athleteId = athleteId;
        this.eventId = eventId;
        this.activityType = activityType;
        this.timestamp = timestamp;
        this.content = content;
    }

    public String getAthleteId() {
        return athleteId;
    }

    public void setAthleteId(String athleteId) {
        this.athleteId = athleteId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getActivityType() {
        return activityType;
    }

    public void setActivityType(String activityType) {
        this.activityType = activityType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    public void setContent(Map<String, Object> content) {
        this.content = content;
    }
}
