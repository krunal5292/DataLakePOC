package org.example.consent.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsentChangedEvent {
    public enum Type {
        GRANTED,
        REVOKED
    }

    private Type type;
    private String athleteId;
    private String purpose; // e.g., "research", "medical"
}
