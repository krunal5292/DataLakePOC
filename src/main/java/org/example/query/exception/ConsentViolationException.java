package org.example.query.exception;

public class ConsentViolationException extends RuntimeException {
    private final String athleteId;
    private final String requestedPurpose;

    public ConsentViolationException(String athleteId, String requestedPurpose) {
        super(String.format("Athlete %s has not consented to purpose: %s", athleteId, requestedPurpose));
        this.athleteId = athleteId;
        this.requestedPurpose = requestedPurpose;
    }

    public String getAthleteId() {
        return athleteId;
    }

    public String getRequestedPurpose() {
        return requestedPurpose;
    }
}
