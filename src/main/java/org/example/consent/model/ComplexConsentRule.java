package org.example.consent.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ComplexConsentRule {
    private String ruleId;
    private String userId; // maps to athleteId
    private String status;
    private Dimensions dimensions;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Dimensions {
        private DimensionDetail purpose;
        private DimensionDetail activity;
        private DimensionDetail events;
        private DimensionDetail fields;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DimensionDetail {
        private String type; // any, specific
        private List<ValueDetail> values;
        private List<FieldSpecification> specifications;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValueDetail {
        private String id;
        private String value;
        private String title;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldSpecification {
        private String fieldName;
        private String operator; // any, range, etc
    }
}
