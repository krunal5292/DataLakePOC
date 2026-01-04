package org.example.ingestion.model;

import java.util.Map;

public class TelemetryMessage {
    private String traceId;
    private String athleteId;
    private long timestamp;
    private Map<String, Object> payload;
    private String minioPath; // Used for downstream saved events

    public TelemetryMessage() {
    }

    public TelemetryMessage(String traceId, String athleteId, long timestamp, Map<String, Object> payload,
            String minioPath) {
        this.traceId = traceId;
        this.athleteId = athleteId;
        this.timestamp = timestamp;
        this.payload = payload;
        this.minioPath = minioPath;
    }

    // Getters
    public String getTraceId() {
        return traceId;
    }

    public String getAthleteId() {
        return athleteId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public String getMinioPath() {
        return minioPath;
    }

    // Setters
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public void setAthleteId(String athleteId) {
        this.athleteId = athleteId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public void setMinioPath(String minioPath) {
        this.minioPath = minioPath;
    }

    // Builder Pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String traceId;
        private String athleteId;
        private long timestamp;
        private Map<String, Object> payload;
        private String minioPath;

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder athleteId(String athleteId) {
            this.athleteId = athleteId;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder payload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        public Builder minioPath(String minioPath) {
            this.minioPath = minioPath;
            return this;
        }

        public TelemetryMessage build() {
            return new TelemetryMessage(traceId, athleteId, timestamp, payload, minioPath);
        }
    }
}
