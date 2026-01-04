package org.example.processing.listener;

import lombok.extern.slf4j.Slf4j;
import org.example.config.KafkaConfig;
import org.example.ingestion.model.TelemetryMessage;
import org.example.ingestion.service.IngestionService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class BronzeConsumer {

    private final IngestionService ingestionService;
    private final KafkaTemplate<String, TelemetryMessage> kafkaTemplate;

    public BronzeConsumer(IngestionService ingestionService, KafkaTemplate<String, TelemetryMessage> kafkaTemplate) {
        this.ingestionService = ingestionService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaConfig.TELEMETRY_INGEST, groupId = "bronze-group")
    public void consume(TelemetryMessage message) {
        log.info("Consuming from Ingest: {}", message.getTraceId());
        try {
            String path = ingestionService.saveToBronze(message);
            
            // Update message with path and publish to next topic
            message.setMinioPath(path);
            kafkaTemplate.send(KafkaConfig.TELEMETRY_BRONZE_SAVED, message.getAthleteId(), message);
            
            log.info("Saved to Bronze and notified: {}", path);
        } catch (Exception e) {
            log.error("Error in BronzeConsumer", e);
            throw e; // Kafka will retry based on config
        }
    }
}
