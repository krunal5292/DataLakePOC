package org.example.query.model;

import java.util.List;

public class GoldDataResponse {
    private String athleteId;
    private List<String> approvedPurposes;
    private List<DataItem> data;
    private int totalRecords;

    public GoldDataResponse() {
    }

    public GoldDataResponse(String athleteId, List<String> approvedPurposes, List<DataItem> data) {
        this.athleteId = athleteId;
        this.approvedPurposes = approvedPurposes;
        this.data = data;
        this.totalRecords = data != null ? data.size() : 0;
    }

    public String getAthleteId() {
        return athleteId;
    }

    public void setAthleteId(String athleteId) {
        this.athleteId = athleteId;
    }

    public List<String> getApprovedPurposes() {
        return approvedPurposes;
    }

    public void setApprovedPurposes(List<String> approvedPurposes) {
        this.approvedPurposes = approvedPurposes;
    }

    public List<DataItem> getData() {
        return data;
    }

    public void setData(List<DataItem> data) {
        this.data = data;
        this.totalRecords = data != null ? data.size() : 0;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }
}
