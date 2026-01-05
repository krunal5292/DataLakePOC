package org.example.processing.service;

import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.strategy.GoldEnforcementStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Facade for Gold layer processing.
 * Delegates to the configured GoldEnforcementStrategy (Physical Partitioning or
 * Bitmask).
 */
@Service
public class GoldService {

    private final GoldEnforcementStrategy strategy;

    @Autowired
    public GoldService(GoldEnforcementStrategy strategy) {
        this.strategy = strategy;
        System.out.println("🎯 Gold Service initialized with strategy: " + strategy.getStrategyName());
    }

    /**
     * Process Silver data and write to Gold based on consent.
     * Delegates to the configured strategy.
     */
    public void processAndFanOut(TelemetryMessage message) {
        strategy.processAndFanOut(message);
    }

    /**
     * Handle consent revocation.
     * Delegates to the configured strategy.
     */
    public void handleRevocation(String athleteId, String purpose) {
        strategy.handleRevocation(athleteId, purpose);
    }

    /**
     * Handle consent grant/re-grant.
     * Delegates to the configured strategy.
     */
    public void handleGrant(String athleteId, String purpose) {
        strategy.handleGrant(athleteId, purpose);
    }

    /**
     * Verify data exists (primarily for testing).
     * Delegates to the configured strategy.
     */
    public boolean verifyDataExists(String athleteId, String purpose) {
        return strategy.verifyDataExists(athleteId, purpose);
    }

    /**
     * Get the active strategy name.
     */
    public String getActiveStrategy() {
        return strategy.getStrategyName();
    }
}
