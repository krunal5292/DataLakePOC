package org.example.query.service;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "iceberg")
public class IcebergQueryService {

    private final Catalog catalog;
    private static final String NAMESPACE = "gold";
    private static final String TABLE_NAME = "telemetry";
    private final TableIdentifier tableIdentifier;

    public IcebergQueryService(Catalog catalog) {
        this.catalog = catalog;
        this.tableIdentifier = TableIdentifier.of(NAMESPACE, TABLE_NAME);
    }

    /**
     * Fetch all records for a specific athlete.
     * 
     * @param athleteId the athlete ID
     * @return List of Iceberg Records
     */
    public List<Record> fetchByAthleteId(String athleteId) {
        return fetchRecords(Expressions.equal("athlete_id", athleteId));
    }

    /**
     * Fetch all records for a specific purpose.
     * 
     * @param purpose the purpose
     * @return List of Iceberg Records
     */
    public List<Record> fetchByPurpose(String purpose) {
        return fetchRecords(Expressions.equal("purpose", purpose));
    }

    /**
     * Fetch records by athlete and purpose.
     * 
     * @param athleteId the athlete ID
     * @param purpose   the purpose
     * @return List of Iceberg Records
     */
    public List<Record> fetchByAthleteIdAndPurpose(String athleteId, String purpose) {
        return fetchRecords(Expressions.and(
                Expressions.equal("athlete_id", athleteId),
                Expressions.equal("purpose", purpose)));
    }

    /**
     * Generic fetch method with complex criteria.
     * 
     * @param athleteIds   list of athlete IDs (optional)
     * @param purposes     list of purposes (optional)
     * @param activityType activity type (optional)
     * @return List of Iceberg Records
     */
    public List<Record> fetchByCriteria(List<String> athleteIds, List<String> purposes, String activityType) {
        Expression expr = Expressions.alwaysTrue();

        if (athleteIds != null && !athleteIds.isEmpty()) {
            Expression athleteExpr = Expressions.alwaysFalse();
            for (String id : athleteIds) {
                athleteExpr = Expressions.or(athleteExpr, Expressions.equal("athlete_id", id));
            }
            expr = Expressions.and(expr, athleteExpr);
        }

        if (purposes != null && !purposes.isEmpty()) {
            Expression purposeExpr = Expressions.alwaysFalse();
            for (String p : purposes) {
                purposeExpr = Expressions.or(purposeExpr, Expressions.equal("purpose", p));
            }
            expr = Expressions.and(expr, purposeExpr);
        }

        if (activityType != null) {
            expr = Expressions.and(expr, Expressions.equal("activity_type", activityType));
        }

        return fetchRecords(expr);
    }

    private List<Record> fetchRecords(Expression finalExpression) {
        if (!catalog.tableExists(tableIdentifier)) {
            System.err.println("Table does not exist: " + tableIdentifier);
            return new ArrayList<>();
        }

        org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        List<Record> records = new ArrayList<>();

        try (CloseableIterable<Record> reader = IcebergGenerics.read(table)
                .where(finalExpression)
                .build()) {
            for (Record record : reader) {
                records.add(record);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to query Iceberg table", e);
        }

        return records;
    }
}
