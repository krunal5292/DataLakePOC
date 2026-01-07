package org.example.processing.service;

import org.springframework.stereotype.Service;

@Service
public class MetadataService {

    public String getAthleteName(String athleteId) {
        // Mock DB lookup
        return "Athlete-" + athleteId.substring(0, 8);
    }

    public String getEventName(String eventId) {
        if (eventId == null) return "Unknown Event";
        // Mock DB lookup
        return eventId.replace("_", " ").toUpperCase();
    }
}
