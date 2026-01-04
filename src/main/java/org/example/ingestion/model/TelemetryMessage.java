package org.example.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryMessage {
    private String traceId;
    private String athleteId;
    private long timestamp;
    private Map<String, Object> payload;
    private String minioPath; // Used for downstream saved events
}
