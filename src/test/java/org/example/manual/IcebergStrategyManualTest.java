package org.example.manual;

import io.minio.MinioClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.strategy.IcebergPhysicalPartitionStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Manual Test for Iceberg Strategy.
 * 
 * Requires LOCAL INFRASTRUCTURE:
 * - MinIO at localhost:9000
 * - Iceberg REST Catalog (if applicable) or S3-based catalog
 * 
 * Run with: mvn test -Dtest=IcebergStrategyManualTest
 * -Dspring.profiles.active=manual
 */
@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("manual")
@org.springframework.test.context.TestPropertySource(properties = "consent.enforcement.strategy=iceberg")
public class IcebergStrategyManualTest {

    @Autowired
    private IcebergPhysicalPartitionStrategy strategy;

    @Autowired
    private MinioClient minioClient;

    private static final String BUCKET_NAME = "gold-warehouse";

    @Autowired
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    // Same schema as integration test
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "trace_id", Types.StringType.get()),
            Types.NestedField.required(2, "athlete_id", Types.StringType.get()),
            Types.NestedField.required(3, "event_id", Types.StringType.get()),
            Types.NestedField.required(4, "timestamp", Types.TimestampType.withZone()),
            Types.NestedField.required(5, "purpose", Types.StringType.get()),
            Types.NestedField.required(6, "activity_type", Types.StringType.get()),
            Types.NestedField.required(7, "heart_rate", Types.IntegerType.get()),
            Types.NestedField.required(8, "latitude", Types.DoubleType.get()),
            Types.NestedField.required(9, "longitude", Types.DoubleType.get()));

    @BeforeEach
    void setup() {
        System.out.println("⚠️ ENSURE LOCAL INFRA IS RUNNING (MinIO:9000) ⚠️");
    }

    @Test
    void testInitialPartitioning() throws Exception {
        System.out.println("Running Manual: testInitialPartitioning");

        String athleteId = "manual-athlete-" + UUID.randomUUID();
        String traceId = UUID.randomUUID().toString();

        // 1. Setup Consent Rule in Redis
        org.example.consent.model.ComplexConsentRule rule = new org.example.consent.model.ComplexConsentRule();
        rule.setUserId(athleteId);
        rule.setStatus("ACTIVE");
        org.example.consent.model.ComplexConsentRule.Dimensions dims = new org.example.consent.model.ComplexConsentRule.Dimensions();
        org.example.consent.model.ComplexConsentRule.DimensionDetail purposeDim = new org.example.consent.model.ComplexConsentRule.DimensionDetail();
        purposeDim.setType("specific");
        purposeDim.setValues(java.util.List.of(
                new org.example.consent.model.ComplexConsentRule.ValueDetail("1", "analytics", "Analytics")));
        dims.setPurpose(purposeDim);
        rule.setDimensions(dims);

        redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));
        System.out.println("✅ Rule set in Redis for: " + athleteId);

        // 2. Setup Data in MinIO (Silver)
        Map<String, Object> payload = new HashMap<>();
        payload.put("event_id", "evt-1");
        payload.put("activity_type", "run");
        payload.put("heart_rate", 150);
        payload.put("latitude", 40.7128);
        payload.put("longitude", -74.0060);

        String silverBucket = "silver";
        if (!minioClient.bucketExists(io.minio.BucketExistsArgs.builder().bucket(silverBucket).build())) {
            minioClient.makeBucket(io.minio.MakeBucketArgs.builder().bucket(silverBucket).build());
        }

        String minioPath = "silver/manual-" + traceId + ".json";
        byte[] dataBytes = objectMapper.writeValueAsBytes(payload);
        minioClient.putObject(io.minio.PutObjectArgs.builder()
                .bucket(silverBucket)
                .object(minioPath)
                .stream(new java.io.ByteArrayInputStream(dataBytes), dataBytes.length, -1)
                .contentType("application/json")
                .build());
        System.out.println("✅ Data uploaded to MinIO: " + minioPath);

        // 3. Process
        TelemetryMessage message = org.example.ingestion.model.TelemetryMessage.builder()
                .athleteId(athleteId)
                .traceId(traceId)
                .timestamp(java.time.Instant.parse("2025-01-01T10:00:00Z").toEpochMilli())
                .payload(payload)
                .minioPath(minioPath)
                .build();

        strategy.processAndFanOut(message);

        // 4. Verify Data Exists via API (or check logs/manual verify)
        boolean exists = strategy.verifyDataExists(athleteId, "analytics");
        assertThat(exists).isTrue();

        System.out.println("✅ Initial Partitioning Verified Locally");
    }

    @Test
    void testReindexing() throws Exception {
        System.out.println("Running Manual: testReindexing");

        // Use a dedicated table for manual reindexing test
        String tableName = "manual_reindex_test";

        // Manual Catalog Setup (mimicking local config)
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "http://localhost:8181"); // Assuming REST Catalog is at 8181 locally
        properties.put("warehouse", "s3://" + BUCKET_NAME + "/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://localhost:9000");
        properties.put("s3.access-key-id", "admin"); // Matches docker-compose
        properties.put("s3.secret-access-key", "password"); // Matches docker-compose
        properties.put("s3.path-style-access", "true");
        properties.put("client.region", "us-east-1");

        Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                new Configuration());

        TableIdentifier id = TableIdentifier.of("gold", tableName);
        if (catalog.tableExists(id))
            catalog.dropTable(id);

        // Define Simple Schema
        Schema simpleSchema = new Schema(
                Types.NestedField.required(1, "department", Types.StringType.get()),
                Types.NestedField.required(2, "region", Types.StringType.get()),
                Types.NestedField.required(3, "data", Types.StringType.get()));
        PartitionSpec specA = PartitionSpec.builderFor(simpleSchema).identity("department").build();
        catalog.createTable(id, simpleSchema, specA);
        Table table = catalog.loadTable(id);

        // Write Data
        GenericRecord record = GenericRecord.create(simpleSchema);
        record.setField("department", "Sales");
        record.setField("region", "US");
        record.setField("data", "v1");

        String filename = "data/" + tableName + "/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + filename);

        try (org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(simpleSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .build()) {
            appender.add(record);
        }

        org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(specA)
                .withPath(outputFile.location())
                .withFileSizeInBytes(100) // Dummy size
                .withRecordCount(1)
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withPartitionPath("department=Sales")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        // Evolve Spec
        table.updateSpec().addField("region").commit();

        // Trigger Re-index (using strategy method - assuming strategy is configured to
        // point to same catalog)
        strategy.reindexData(tableName);

        // Limit Verification via Catalog to strict logic
        Table reloaded = catalog.loadTable(id);
        try (CloseableIterable<FileScanTask> tasks = reloaded.newScan().planFiles()) {
            boolean foundNewPartition = false;
            for (FileScanTask task : tasks) {
                String dept = task.file().partition().get(0, String.class);
                String reg = task.file().partition().get(1, String.class);

                assertThat(dept).isEqualTo("Sales");
                assertThat(reg).isEqualTo("US");
                foundNewPartition = true;
            }
            assertThat(foundNewPartition).isTrue();
        }

        System.out.println("✅ Re-indexing Verified Successfully");
    }
}
