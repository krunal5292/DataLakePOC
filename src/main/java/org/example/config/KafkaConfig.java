package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String TELEMETRY_INGEST = "telemetry-ingest";
    public static final String TELEMETRY_BRONZE_SAVED = "telemetry-bronze-saved";
    public static final String TELEMETRY_SILVER_SAVED = "telemetry-silver-saved";

    @Bean
    public NewTopic telemetryIngest() {
        return TopicBuilder.name(TELEMETRY_INGEST)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic telemetryBronzeSaved() {
        return TopicBuilder.name(TELEMETRY_BRONZE_SAVED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic telemetrySilverSaved() {
        return TopicBuilder.name(TELEMETRY_SILVER_SAVED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    public static final String CONSENT_EVENTS = "consent-events";

    @Bean
    public NewTopic consentEvents() {
        return TopicBuilder.name(CONSENT_EVENTS)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
