package org.example.consent.model;

public class ConsentChangedEvent {
    public enum Type {
        GRANTED,
        REVOKED
    }

    private Type type;
    private String athleteId;
    private String purpose; // e.g., "research", "medical"

    public ConsentChangedEvent() {
    }

    public ConsentChangedEvent(Type type, String athleteId, String purpose) {
        this.type = type;
        this.athleteId = athleteId;
        this.purpose = purpose;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getAthleteId() {
        return athleteId;
    }

    public void setAthleteId(String athleteId) {
        this.athleteId = athleteId;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    @Override
    public String toString() {
        return "ConsentChangedEvent{type=" + type + ", athleteId='" + athleteId + "', purpose='" + purpose + "'}";
    }
}
