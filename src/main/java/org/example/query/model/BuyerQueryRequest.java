package org.example.query.model;

/**
 * Request model for buyer data queries.
 */
public class BuyerQueryRequest {

    private String purpose;
    private String athleteId;
    private String eventId;
    private String activityType;

    public BuyerQueryRequest() {
    }

    public BuyerQueryRequest(String purpose) {
        this.purpose = purpose;
    }

    public BuyerQueryRequest(String purpose, String athleteId, String eventId, String activityType) {
        this.purpose = purpose;
        this.athleteId = athleteId;
        this.eventId = eventId;
        this.activityType = activityType;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
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
}
