package org.example.processing.listener;

import lombok.extern.slf4j.Slf4j;
import org.example.config.KafkaConfig;
import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.service.GoldService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class GoldConsentProcessor {

    private final GoldService goldService;

    public GoldConsentProcessor(GoldService goldService) {
        this.goldService = goldService;
    }

    @KafkaListener(topics = KafkaConfig.TELEMETRY_SILVER_SAVED, groupId = "gold-group")
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
