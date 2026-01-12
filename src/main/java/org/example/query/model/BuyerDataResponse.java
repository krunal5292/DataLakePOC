package org.example.query.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Response model for buyer data queries.
 */
public class BuyerDataResponse {

    private String purpose;
    private int totalRecords;
    private List<BuyerDataItem> data;

    public BuyerDataResponse() {
        this.data = new ArrayList<>();
    }

    public BuyerDataResponse(String purpose, List<BuyerDataItem> data) {
        this.purpose = purpose;
        this.data = data != null ? data : new ArrayList<>();
        this.totalRecords = this.data.size();
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public List<BuyerDataItem> getData() {
        return data;
    }

    public void setData(List<BuyerDataItem> data) {
        this.data = data;
        this.totalRecords = data != null ? data.size() : 0;
    }
}
