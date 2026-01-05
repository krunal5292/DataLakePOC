package org.example.consent.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.consent.model.ComplexConsentRule;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/consent")
public class ConsentController {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public ConsentController(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/complex")
    public String createComplexRule(@RequestBody ComplexConsentRule rule) throws Exception {
        String json = objectMapper.writeValueAsString(rule);
        redisTemplate.opsForValue().set("consent:rule:" + rule.getUserId(), json);
        return "SAVED";
    }
}
