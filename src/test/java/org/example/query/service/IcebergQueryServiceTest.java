package org.example.query.service;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.example.processing.strategy.IcebergPhysicalPartitionStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergQueryServiceTest {

    @TempDir
    File tempDir;

    private Catalog catalog;
    private IcebergQueryService queryService;
    private static final String NAMESPACE = "gold";
    private static final String TABLE_NAME = "telemetry";

    @BeforeEach
    void setUp() {
        // Initialize InMemoryCatalog for local testing
        Catalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize("test-catalog", new HashMap<>() {
            {
                put("warehouse", "file://" + tempDir.getAbsolutePath());
            }
        });
        this.catalog = inMemoryCatalog;

        this.queryService = new IcebergQueryService(catalog);

        // Ensure namespace exists
        Namespace namespace = Namespace.of(NAMESPACE);
        if (inMemoryCatalog instanceof SupportsNamespaces) {
            try {
                ((SupportsNamespaces) inMemoryCatalog).createNamespace(namespace);
            } catch (Exception e) {
                // Ignore if exists (though strictly new instance shouldn't have it)
            }
        }

        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, TABLE_NAME);
        catalog.createTable(tableId, IcebergPhysicalPartitionStrategy.SCHEMA);
    }

    @Test
    void testFetchByAthleteId() throws Exception {
        // Arrange: Write data
        writeRecord("ath-1", "training", "run");
        writeRecord("ath-1", "research", "run");
        writeRecord("ath-2", "training", "swim");

        // Act
        List<Record> results = queryService.fetchByAthleteId("ath-1");

        // Assert
        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(r -> "ath-1".equals(r.getField("athlete_id"))));
    }

    @Test
    void testFetchByPurpose() throws Exception {
        // Arrange
        writeRecord("ath-1", "training", "run");
        writeRecord("ath-2", "training", "swim");
        writeRecord("ath-1", "research", "run");

        // Act
        List<Record> results = queryService.fetchByPurpose("training");

        // Assert
        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(r -> "training".equals(r.getField("purpose"))));
    }

    @Test
    void testFetchByCriteria() throws Exception {
        // Arrange
        writeRecord("ath-1", "training", "run");
        writeRecord("ath-1", "training", "cycle");
        writeRecord("ath-2", "training", "run");
        writeRecord("ath-3", "research", "run");

        // Act: Filter by ath-1 OR ath-2 AND activity=run
        List<Record> results = queryService.fetchByCriteria(
                List.of("ath-1", "ath-2"),
                null,
                "run");

        // Assert
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(r -> "ath-1".equals(r.getField("athlete_id"))));
        assertTrue(results.stream().anyMatch(r -> "ath-2".equals(r.getField("athlete_id"))));
        assertTrue(results.stream().allMatch(r -> "run".equals(r.getField("activity_type"))));
    }

    private void writeRecord(String athleteId, String purpose, String activityType) throws Exception {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, TABLE_NAME);
        Table table = catalog.loadTable(tableId);

        GenericRecord record = GenericRecord.create(IcebergPhysicalPartitionStrategy.SCHEMA);
        record.setField("trace_id", UUID.randomUUID().toString());
        record.setField("athlete_id", athleteId);
        record.setField("timestamp", OffsetDateTime.now(ZoneOffset.UTC));
        record.setField("purpose", purpose);
        record.setField("activity_type", activityType);
        record.setField("event_id", UUID.randomUUID().toString());
        record.setField("data", "{}");

        // Simple write using IcebergGenerics (if supported by HadoopCatalog/FileIO
        // properly)
        // or Manual file creation. For unit tests, appendFile is standard.
        // But to simplify, let's use DataFiles builder with a dummy path if we mock,
        // OR better: use actual parquet writer.

        String filename = UUID.randomUUID().toString() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/data/" + filename);

        FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        appender.add(record);
        appender.close();

        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(outputFile.location())
                .withFileSizeInBytes(appender.length())
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .build();

        table.newAppend().appendFile(dataFile).commit();
    }
}
