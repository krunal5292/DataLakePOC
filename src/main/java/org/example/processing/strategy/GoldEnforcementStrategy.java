package org.example.processing.strategy;

import org.example.ingestion.model.TelemetryMessage;

/**
 * Strategy interface for Gold layer consent enforcement.
 * Implementations can use different approaches (Physical Partitioning, Bitmask,
 * etc.)
 * to enforce consent rules while maintaining the same interface.
 */
public interface GoldEnforcementStrategy {

    /**
     * Process a Silver layer message and write to Gold based on consent rules.
     * This is the core "fan-out" logic that duplicates data across allowed
     * purposes.
     *
     * @param message The telemetry message from Silver layer
     */
    void processAndFanOut(TelemetryMessage message);

    /**
     * Handle consent revocation for a specific purpose.
     * Implementations should make the data immediately inaccessible.
     *
     * @param athleteId The athlete whose consent was revoked
     * @param purpose   The purpose that was revoked
     */
    void handleRevocation(String athleteId, String purpose);

    /**
     * Handle consent grant (or re-grant) for a specific purpose.
     * Implementations should backfill historical data from Silver layer.
     *
     * @param athleteId The athlete who granted consent
     * @param purpose   The purpose that was granted
     */
    void handleGrant(String athleteId, String purpose);

    /**
     * Verify that data exists for a given athlete and purpose.
     * Used primarily for testing to validate consent enforcement.
     *
     * @param athleteId The athlete ID
     * @param purpose   The purpose to check
     * @return true if data exists and is accessible, false otherwise
     */
    boolean verifyDataExists(String athleteId, String purpose);

    /**
     * Get the name of this strategy (e.g., "partition", "bitmask").
     *
     * @return Strategy name
     */
    String getStrategyName();
}
