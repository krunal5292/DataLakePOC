package org.example.query.controller;

import org.example.query.model.GoldDataResponse;
import org.example.query.service.GoldDataQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/query")
public class GoldDataQueryController {

    private final GoldDataQueryService queryService;

    public GoldDataQueryController(GoldDataQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ResponseEntity<GoldDataResponse> queryData(
            @RequestParam String athleteId,
            @RequestParam List<String> purposes) {
        try {
            GoldDataResponse response = queryService.queryAthleteData(athleteId, purposes);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
