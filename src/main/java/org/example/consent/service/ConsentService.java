package org.example.consent.service;

import org.example.consent.model.ConsentRule;
import org.example.consent.repository.ConsentRuleRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConsentService {

    private final ConsentRuleRepository repository;
    private final BitmaskGenerator bitmaskGenerator;
    private final StringRedisTemplate redisTemplate;
    private final ClickHouseService clickHouseService;

    public ConsentService(ConsentRuleRepository repository, BitmaskGenerator bitmaskGenerator, StringRedisTemplate redisTemplate, ClickHouseService clickHouseService) {
        this.repository = repository;
        this.bitmaskGenerator = bitmaskGenerator;
        this.redisTemplate = redisTemplate;
        this.clickHouseService = clickHouseService;
    }

    @Transactional
    public ConsentRule saveRule(ConsentRule rule) {
        rule.setStatus("ACTIVE");
        ConsentRule saved = repository.save(rule);
        
        long mask = bitmaskGenerator.generateMask(saved);
        
        // Update Redis: athlete_id -> bitmask
        redisTemplate.opsForValue().set("consent:" + saved.getAthleteId(), String.valueOf(mask));
        
        // Trigger ClickHouse ALTER TABLE UPDATE for existing rows
        updateClickHouseMask(saved.getAthleteId(), mask);
        
        return saved;
    }

    private void updateClickHouseMask(String athleteId, long mask) {
        String sql = "ALTER TABLE athlete_data_gold UPDATE consent_mask = ? WHERE athlete_id = ?";
        clickHouseService.executeUpdate(sql, mask, athleteId);
        System.out.println("Updating ClickHouse for athlete " + athleteId + " with mask " + mask);
    }

    @Transactional
    public void revokeConsent(String athleteId) {
        repository.findByAthleteIdAndStatus(athleteId, "ACTIVE").ifPresent(rule -> {
            rule.setStatus("INACTIVE");
            repository.save(rule);
            
            // Flush Redis
            redisTemplate.delete("consent:" + athleteId);
            
            // Update ClickHouse with 0 mask (no access)
            updateClickHouseMask(athleteId, 0L);
            
            System.out.println("Revoked consent for athlete " + athleteId);
        });
    }
}
