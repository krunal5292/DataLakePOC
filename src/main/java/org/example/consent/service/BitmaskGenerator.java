package org.example.consent.service;

import org.example.consent.model.ConsentRule;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class BitmaskGenerator {

    private static final Map<String, Integer> BIT_MAPPING = new HashMap<>();

    static {
        // Purpose
        BIT_MAPPING.put("RESEARCH", 0);
        BIT_MAPPING.put("COMMERCIAL", 1);
        
        // Activity
        BIT_MAPPING.put("RUNNING", 5);
        BIT_MAPPING.put("CYCLING", 6);
        
        // Fields
        BIT_MAPPING.put("HEART_RATE", 10);
        BIT_MAPPING.put("BLOOD_PRESSURE", 11);
    }

    public long generateMask(ConsentRule rule) {
        long mask = 0;
        
        if (rule.getPurposes() != null) {
            for (String purpose : rule.getPurposes()) {
                Integer bit = BIT_MAPPING.get(purpose);
                if (bit != null) mask |= (1L << bit);
            }
        }
        
        if (rule.getActivities() != null) {
            for (String activity : rule.getActivities()) {
                Integer bit = BIT_MAPPING.get(activity);
                if (bit != null) mask |= (1L << bit);
            }
        }
        
        if (rule.getFields() != null) {
            for (String field : rule.getFields()) {
                Integer bit = BIT_MAPPING.get(field);
                if (bit != null) mask |= (1L << bit);
            }
        }
        
        return mask;
    }
    
    public long generateRequiredMask(java.util.List<String> purposes, java.util.List<String> activities, java.util.List<String> fields) {
        long mask = 0;
        if (purposes != null) {
            for (String p : purposes) {
                Integer bit = BIT_MAPPING.get(p);
                if (bit != null) mask |= (1L << bit);
            }
        }
        if (activities != null) {
            for (String a : activities) {
                Integer bit = BIT_MAPPING.get(a);
                if (bit != null) mask |= (1L << bit);
            }
        }
        if (fields != null) {
            for (String f : fields) {
                Integer bit = BIT_MAPPING.get(f);
                if (bit != null) mask |= (1L << bit);
            }
        }
        return mask;
    }
}
