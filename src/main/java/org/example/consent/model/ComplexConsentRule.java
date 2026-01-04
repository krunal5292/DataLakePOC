package org.example.consent.model;

import java.util.List;

public class ComplexConsentRule {
    private String ruleId;
    private String userId; // maps to athleteId
    private String status;
    private Dimensions dimensions;

    public ComplexConsentRule() {
    }

    public ComplexConsentRule(String ruleId, String userId, String status, Dimensions dimensions) {
        this.ruleId = ruleId;
        this.userId = userId;
        this.status = status;
        this.dimensions = dimensions;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Dimensions getDimensions() {
        return dimensions;
    }

    public void setDimensions(Dimensions dimensions) {
        this.dimensions = dimensions;
    }

    public static class Dimensions {
        private DimensionDetail purpose;
        private DimensionDetail activity;
        private DimensionDetail events;
        private DimensionDetail fields;

        public Dimensions() {
        }

        public Dimensions(DimensionDetail purpose, DimensionDetail activity, DimensionDetail events,
                DimensionDetail fields) {
            this.purpose = purpose;
            this.activity = activity;
            this.events = events;
            this.fields = fields;
        }

        public DimensionDetail getPurpose() {
            return purpose;
        }

        public void setPurpose(DimensionDetail purpose) {
            this.purpose = purpose;
        }

        public DimensionDetail getActivity() {
            return activity;
        }

        public void setActivity(DimensionDetail activity) {
            this.activity = activity;
        }

        public DimensionDetail getEvents() {
            return events;
        }

        public void setEvents(DimensionDetail events) {
            this.events = events;
        }

        public DimensionDetail getFields() {
            return fields;
        }

        public void setFields(DimensionDetail fields) {
            this.fields = fields;
        }
    }

    public static class DimensionDetail {
        private String type; // any, specific
        private List<ValueDetail> values;
        private List<FieldSpecification> specifications;

        public DimensionDetail() {
        }

        public DimensionDetail(String type, List<ValueDetail> values, List<FieldSpecification> specifications) {
            this.type = type;
            this.values = values;
            this.specifications = specifications;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<ValueDetail> getValues() {
            return values;
        }

        public void setValues(List<ValueDetail> values) {
            this.values = values;
        }

        public List<FieldSpecification> getSpecifications() {
            return specifications;
        }

        public void setSpecifications(List<FieldSpecification> specifications) {
            this.specifications = specifications;
        }
    }

    public static class ValueDetail {
        private String id;
        private String value;
        private String title;

        public ValueDetail() {
        }

        public ValueDetail(String id, String value, String title) {
            this.id = id;
            this.value = value;
            this.title = title;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }
    }

    public static class FieldSpecification {
        private String fieldName;
        private String operator; // any, range, etc

        public FieldSpecification() {
        }

        public FieldSpecification(String fieldName, String operator) {
            this.fieldName = fieldName;
            this.operator = operator;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }
    }
}
