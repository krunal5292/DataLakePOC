package org.example.consent.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.consent.model.ComplexConsentRule;
import org.example.consent.model.ConsentRule;
import org.example.consent.service.ConsentService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/consent")
public class ConsentController {

    private final ConsentService consentService;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public ConsentController(ConsentService consentService, StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.consentService = consentService;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ConsentRule createRule(@RequestBody ConsentRule rule) {
        return consentService.saveRule(rule);
    }

    @PostMapping("/complex")
    public String createComplexRule(@RequestBody ComplexConsentRule rule) throws Exception {
        String json = objectMapper.writeValueAsString(rule);
        redisTemplate.opsForValue().set("consent:rule:" + rule.getUserId(), json);
        return "SAVED";
    }

    @DeleteMapping("/{athleteId}")
    public void revokeConsent(@PathVariable String athleteId) {
        consentService.revokeConsent(athleteId);
    }
}
