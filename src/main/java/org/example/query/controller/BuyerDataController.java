package org.example.query.controller;

import org.example.query.model.BuyerDataResponse;
import org.example.query.strategy.BuyerQueryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST controller for buyer data access.
 * Provides endpoints for buyers to fetch data based on purpose.
 */
@RestController
@RequestMapping("/api/buyer")
public class BuyerDataController {

    private static final Logger log = LoggerFactory.getLogger(BuyerDataController.class);

    private final BuyerQueryStrategy queryStrategy;

    public BuyerDataController(BuyerQueryStrategy queryStrategy) {
        this.queryStrategy = queryStrategy;
    }

    /**
     * Fetch data for a specific purpose.
     * 
     * @param purpose the purpose for which data is requested
     * @return BuyerDataResponse containing all data for the purpose
     */
    @GetMapping("/data")
    public ResponseEntity<BuyerDataResponse> fetchDataByPurpose(@RequestParam String purpose) {
        log.info("Buyer API: Fetching data for purpose: {} using strategy: {}",
                purpose, queryStrategy.getStrategyName());

        if (purpose == null || purpose.trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        try {
            BuyerDataResponse response = queryStrategy.fetchByPurpose(purpose);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error fetching data for purpose: {}", purpose, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Fetch data for multiple purposes (batch request).
     * 
     * @param request BuyerQueryRequest containing list of purposes
     * @return BuyerDataResponse containing data for all purposes
     */
    @PostMapping("/data/batch")
    public ResponseEntity<BuyerDataResponse> fetchDataBatch(@RequestBody List<String> purposes) {
        log.info("Buyer API: Fetching data for {} purposes using strategy: {}",
                purposes.size(), queryStrategy.getStrategyName());

        if (purposes == null || purposes.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        try {
            BuyerDataResponse response = queryStrategy.fetchByPurposes(purposes);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error fetching data for purposes: {}", purposes, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Health check endpoint to verify which strategy is active.
     */
    @GetMapping("/strategy")
    public ResponseEntity<String> getActiveStrategy() {
        return ResponseEntity.ok(queryStrategy.getStrategyName());
    }
}
