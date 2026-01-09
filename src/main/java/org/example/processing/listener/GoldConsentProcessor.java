package org.example.processing.listener;

import org.example.config.KafkaConfig;
import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.service.GoldService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class GoldConsentProcessor {

    private static final Logger log = LoggerFactory.getLogger(GoldConsentProcessor.class);

    private final GoldService goldService;

    public GoldConsentProcessor(GoldService goldService) {
        this.goldService = goldService;
    }

    @KafkaListener(topics = KafkaConfig.TELEMETRY_SILVER_SAVED, groupId = "${spring.kafka.consumer.group-id.gold:gold-group}")
    public void consume(TelemetryMessage message) {
        log.info("Fanning out to Gold based on Consent: {}", message.getTraceId());
        try {
            goldService.processAndFanOut(message);
        } catch (Exception e) {
            log.error("Error in GoldConsentProcessor", e);
            throw e;
        }
    }
}
