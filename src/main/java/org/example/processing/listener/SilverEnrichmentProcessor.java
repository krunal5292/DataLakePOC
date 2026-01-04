package org.example.processing.listener;

import org.example.config.KafkaConfig;
import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.service.SilverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class SilverEnrichmentProcessor {

    private static final Logger log = LoggerFactory.getLogger(SilverEnrichmentProcessor.class);

    private final SilverService silverService;
    private final KafkaTemplate<String, TelemetryMessage> kafkaTemplate;

    public SilverEnrichmentProcessor(SilverService silverService, KafkaTemplate<String, TelemetryMessage> kafkaTemplate) {
        this.silverService = silverService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaConfig.TELEMETRY_BRONZE_SAVED, groupId = "silver-group")
    public void consume(TelemetryMessage message) {
        log.info("Enriching Silver: {}", message.getTraceId());
        try {
            String path = silverService.processAndSave(message);
            
            message.setMinioPath(path);
            kafkaTemplate.send(KafkaConfig.TELEMETRY_SILVER_SAVED, message.getAthleteId(), message);
            
            log.info("Saved to Silver and notified: {}", path);
        } catch (Exception e) {
            log.error("Error in SilverEnrichmentProcessor", e);
            throw e;
        }
    }
}
