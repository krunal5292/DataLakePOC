package org.example.consent.service;

import org.example.consent.model.ConsentRule;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BitmaskGeneratorTest {

    private final BitmaskGenerator generator = new BitmaskGenerator();

    @Test
    public void testGenerateMask() {
        ConsentRule rule = new ConsentRule();
        rule.setPurposes(List.of("RESEARCH"));
        rule.setActivities(List.of("RUNNING"));
        rule.setFields(List.of("HEART_RATE"));
        
        // Bit 0: RESEARCH (1)
        // Bit 5: RUNNING (32)
        // Bit 10: HEART_RATE (1024)
        // Total: 1057
        
        long mask = generator.generateMask(rule);
        assertEquals(1057L, mask);
    }

    @Test
    public void testGenerateRequiredMask() {
        // Same as above
        long mask = generator.generateRequiredMask(List.of("RESEARCH"), List.of("RUNNING"), List.of("HEART_RATE"));
        assertEquals(1057L, mask);
    }
}
