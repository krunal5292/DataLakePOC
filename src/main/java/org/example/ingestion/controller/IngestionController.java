package org.example.ingestion.controller;

import org.example.config.KafkaConfig;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/ingest")
public class IngestionController {

    private final KafkaTemplate<String, TelemetryMessage> kafkaTemplate;

    public IngestionController(KafkaTemplate<String, TelemetryMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> ingestTelemetry(@RequestBody Map<String, Object> payload) {
        String athleteId = (String) payload.get("athlete_id");
        if (athleteId == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "athlete_id is required"));
        }

        String traceId = UUID.randomUUID().toString();
        
        TelemetryMessage message = TelemetryMessage.builder()
                .traceId(traceId)
                .athleteId(athleteId)
                .timestamp(System.currentTimeMillis())
                .payload(payload)
                .build();

        kafkaTemplate.send(KafkaConfig.TELEMETRY_INGEST, athleteId, message);

        return ResponseEntity.accepted().body(Map.of(
                "trace_id", traceId,
                "status", "ACCEPTED"
        ));
    }
}
