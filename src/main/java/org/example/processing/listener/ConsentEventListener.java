package org.example.processing.listener;

import lombok.extern.slf4j.Slf4j;
import org.example.config.KafkaConfig;
import org.example.consent.model.ConsentChangedEvent;
import org.example.processing.service.GoldIndexManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsentEventListener {

    private final GoldIndexManager goldIndexManager;

    public ConsentEventListener(GoldIndexManager goldIndexManager) {
        this.goldIndexManager = goldIndexManager;
    }

    @KafkaListener(topics = KafkaConfig.CONSENT_EVENTS, groupId = "gold-index-manager")
    public void consume(ConsentChangedEvent event) {
        log.info("Received Consent Event: {}", event);
        try {
            goldIndexManager.handleEvent(event);
        } catch (Exception e) {
            log.error("Error processing consent event", e);
            // In prod: Dead Letter Queue
        }
    }
}
