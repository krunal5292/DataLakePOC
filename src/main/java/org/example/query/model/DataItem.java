package org.example.query.model;

import java.util.Map;

public class DataItem {
    private String purpose;
    private String objectPath;
    private String timestamp;
    private Map<String, Object> content;

    public DataItem() {
    }

    public DataItem(String purpose, String objectPath, String timestamp, Map<String, Object> content) {
        this.purpose = purpose;
        this.objectPath = objectPath;
        this.timestamp = timestamp;
        this.content = content;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    public void setContent(Map<String, Object> content) {
        this.content = content;
    }
}
