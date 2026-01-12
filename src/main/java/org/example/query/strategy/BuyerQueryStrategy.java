package org.example.query.strategy;

import org.example.query.model.BuyerDataResponse;

import java.util.List;

/**
 * Strategy interface for buyer data queries.
 * Implementations provide different query mechanisms (partition-based,
 * Iceberg-based)
 * based on the gold layer storage strategy.
 */
public interface BuyerQueryStrategy {

    /**
     * Fetch data for a specific purpose.
     * 
     * @param purpose the purpose for which data is requested
     * @return BuyerDataResponse containing all data for the purpose
     */
    BuyerDataResponse fetchByPurpose(String purpose);

    /**
     * Fetch data for multiple purposes.
     * 
     * @param purposes list of purposes
     * @return BuyerDataResponse containing data for all purposes
     */
    BuyerDataResponse fetchByPurposes(List<String> purposes);

    /**
     * Get the name of this strategy.
     * 
     * @return strategy name (e.g., "partition", "iceberg")
     */
    String getStrategyName();
}
