package org.example.consent.controller;

import org.example.consent.service.BitmaskGenerator;
import org.example.consent.service.ClickHouseService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/buyer")
public class BuyerController {

    private final BitmaskGenerator bitmaskGenerator;
    private final ClickHouseService clickHouseService;

    public BuyerController(BitmaskGenerator bitmaskGenerator, ClickHouseService clickHouseService) {
        this.bitmaskGenerator = bitmaskGenerator;
        this.clickHouseService = clickHouseService;
    }

    @GetMapping("/data")
    public List<Map<String, Object>> getData(
            @RequestParam(required = false) List<String> purposes,
            @RequestParam(required = false) List<String> activities,
            @RequestParam(required = false) List<String> fields) {
        
        if ((purposes == null || purposes.isEmpty()) && 
            (activities == null || activities.isEmpty()) && 
            (fields == null || fields.isEmpty())) {
            return List.of();
        }
        
        long requiredMask = bitmaskGenerator.generateRequiredMask(purposes, activities, fields);
        if (requiredMask == 0) {
            return List.of();
        }
        
        return clickHouseService.fetchData(requiredMask);
    }
}
