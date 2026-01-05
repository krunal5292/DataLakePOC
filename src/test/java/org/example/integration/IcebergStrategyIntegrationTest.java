package org.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.example.consent.model.ComplexConsentRule;
import org.example.ingestion.model.TelemetryMessage;
import org.example.processing.strategy.IcebergPhysicalPartitionStrategy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "consent.enforcement.strategy=iceberg")
public class IcebergStrategyIntegrationTest extends BaseTestcontainersTest {

        // Postgres for Iceberg Catalog
        private static final GenericContainer<?> POSTGRES = new GenericContainer<>(
                        DockerImageName.parse("postgres:15-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("db-catalog")
                        .withExposedPorts(5432)
                        .withEnv("POSTGRES_USER", "admin")
                        .withEnv("POSTGRES_PASSWORD", "password")
                        .withEnv("POSTGRES_DB", "iceberg");

        private static final GenericContainer<?> ICEBERG_REST = new GenericContainer<>(
                        DockerImageName.parse("tabulario/iceberg-rest"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("iceberg-rest")
                        .withExposedPorts(8181)
                        .withEnv("AWS_REGION", "us-east-1")
                        .withEnv("CATALOG_WAREHOUSE", "s3://gold-warehouse/")
                        .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                        .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000") // Uses network alias from
                                                                             // BaseTestcontainersTest
                        .withEnv("CATALOG_S3_ACCESS__KEY__ID", "admin")
                        .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", "password")
                        .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                        .withEnv("JDBC_USER", "admin")
                        .withEnv("JDBC_PASSWORD", "password")
                        .withEnv("JDBC_DB", "iceberg")
                        .dependsOn(POSTGRES); // MinIO is already started in Base

        @BeforeAll
        static void startIceberg() {
                POSTGRES.start();
                ICEBERG_REST.start();
        }

        // No @AfterAll needed as Testcontainers handles shutdown, or Base handles
        // shared ones.
        // Specifying stop explicitly might be safer if we don't want them leaking, but
        // usually fine.

        @DynamicPropertySource
        static void icebergProperties(DynamicPropertyRegistry registry) {
                // Base properties (Redis, MinIO, Kafka) are loaded by BaseTestcontainersTest

                registry.add("iceberg.catalog.uri",
                                () -> "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());

                // We must point the App to the MinIO container's external port
                registry.add("iceberg.s3.endpoint",
                                () -> "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                registry.add("iceberg.s3.access-key", () -> "admin");
                registry.add("iceberg.s3.secret-key", () -> "password");
        }

        @Autowired
        private IcebergPhysicalPartitionStrategy strategy;

        @Autowired
        private MinioClient minioClient;

        @Autowired
        private ObjectMapper objectMapper;

        @Autowired
        private StringRedisTemplate redisTemplate;

        @Test
        void testFanOutCreatesCorrectPartitions() throws Exception {
                // 1. Setup Data
                String athleteId = "athlete1";
                String traceId = UUID.randomUUID().toString();
                long timestamp = Instant.parse("2026-01-05T10:00:00Z").toEpochMilli();

                // Consent Rule: Research(Any), Marketing(Any)
                ComplexConsentRule rule = new ComplexConsentRule();
                rule.setUserId(athleteId);
                rule.setStatus("ACTIVE");
                ComplexConsentRule.Dimensions dims = new ComplexConsentRule.Dimensions();
                ComplexConsentRule.DimensionDetail purposeDim = new ComplexConsentRule.DimensionDetail();
                purposeDim.setType("specific");
                purposeDim.setValues(List.of(
                                new ComplexConsentRule.ValueDetail("1", "research", "Research"),
                                new ComplexConsentRule.ValueDetail("2", "marketing", "Marketing")));
                dims.setPurpose(purposeDim);

                // Allowed Event
                ComplexConsentRule.DimensionDetail eventDim = new ComplexConsentRule.DimensionDetail();
                eventDim.setType("any");
                dims.setEvents(eventDim);

                rule.setDimensions(dims);
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // Enriched Data in Silver
                Map<String, Object> payload = new HashMap<>();
                payload.put("hr", 140);
                payload.put("activity_type", "running");
                payload.put("event_id", "training_session_1");

                String silverPath = "silver/data.json";
                if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("silver").build())) {
                        minioClient.makeBucket(MakeBucketArgs.builder().bucket("silver").build());
                }

                byte[] dataBytes = objectMapper.writeValueAsBytes(payload);
                minioClient.putObject(PutObjectArgs.builder()
                                .bucket("silver")
                                .object(silverPath)
                                .stream(new ByteArrayInputStream(dataBytes), dataBytes.length, -1)
                                .build());

                TelemetryMessage message = new TelemetryMessage(traceId, athleteId, timestamp, payload, silverPath);

                // 2. Execute Fan-Out
                strategy.processAndFanOut(message);

                // 3. Verify Iceberg Data
                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                // External access to MinIO
                properties.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");

                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                                new org.apache.hadoop.conf.Configuration());
                Table table = catalog.loadTable(TableIdentifier.of("gold", "telemetry"));

                // Verify Research
                CloseableIterable<Record> researchRecords = IcebergGenerics.read(table)
                                .where(org.apache.iceberg.expressions.Expressions.equal("purpose", "research"))
                                .build();
                assertThat(researchRecords).hasSize(1);
                Record r1 = researchRecords.iterator().next();
                assertThat(r1.getField("activity_type")).isEqualTo("running");
                assertThat(r1.getField("athlete_id")).isEqualTo(athleteId);

                // Verify Marketing
                CloseableIterable<Record> marketingRecords = IcebergGenerics.read(table)
                                .where(org.apache.iceberg.expressions.Expressions.equal("purpose", "marketing"))
                                .build();
                assertThat(marketingRecords).hasSize(1);
        }

        @Test
        void testRevocationRemovesData() throws Exception {
                String athleteId = "athlete_revoke";
                String traceId = UUID.randomUUID().toString();
                long timestamp = Instant.now().toEpochMilli();

                // Consent to Research
                ComplexConsentRule rule = new ComplexConsentRule();
                rule.setUserId(athleteId);
                rule.setStatus("ACTIVE");
                ComplexConsentRule.Dimensions dims = new ComplexConsentRule.Dimensions();
                dims.setPurpose(new ComplexConsentRule.DimensionDetail("specific",
                                List.of(new ComplexConsentRule.ValueDetail("1", "research", "Research")), null));
                dims.setEvents(new ComplexConsentRule.DimensionDetail("any", null, null));
                rule.setDimensions(dims);
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // Enriched Data
                Map<String, Object> payload = new HashMap<>();
                payload.put("activity_type", "cycling");
                String silverPath = "silver/revoke_data.json";

                if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("silver").build())) {
                        minioClient.makeBucket(MakeBucketArgs.builder().bucket("silver").build());
                }

                byte[] dataBytes = objectMapper.writeValueAsBytes(payload);
                minioClient.putObject(PutObjectArgs.builder().bucket("silver").object(silverPath)
                                .stream(new ByteArrayInputStream(dataBytes), dataBytes.length, -1).build());

                TelemetryMessage message = new TelemetryMessage(traceId, athleteId, timestamp, payload, silverPath);
                strategy.processAndFanOut(message);

                assertThat(strategy.verifyDataExists(athleteId, "research")).isTrue();

                // 2. Revoke
                strategy.handleRevocation(athleteId, "research");

                // 3. Verify Gone
                assertThat(strategy.verifyDataExists(athleteId, "research")).isFalse();
        }

        @Test
        void testDataMartCreation() throws Exception {
                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");
                // properties.put("client.region", "us-east-1"); // Needed for some clients

                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                                new org.apache.hadoop.conf.Configuration());

                TableIdentifier martId = TableIdentifier.of("gold", "megacorp");
                if (catalog.tableExists(martId))
                        catalog.dropTable(martId);

                // Different Partition Spec (by activity)
                org.apache.iceberg.types.Types.NestedField[] fields = {
                                org.apache.iceberg.types.Types.NestedField.required(1, "athlete_id",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(2, "activity",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(3, "data",
                                                org.apache.iceberg.types.Types.StringType.get())
                };
                Schema martSchema = new Schema(fields);
                org.apache.iceberg.PartitionSpec martSpec = org.apache.iceberg.PartitionSpec.builderFor(martSchema)
                                .identity("activity")
                                .build();

                catalog.createTable(martId, martSchema, martSpec);
                Table martTable = catalog.loadTable(martId);

                // Populate manual write
                GenericRecord record = GenericRecord.create(martSchema);
                record.setField("athlete_id", "athlete1");
                record.setField("activity", "running");
                record.setField("data", "{}");

                String filename = "data/megacorp/" + UUID.randomUUID() + ".parquet";
                OutputFile outputFile = martTable.io().newOutputFile(martTable.location() + "/" + filename);

                org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                                .schema(martSchema)
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite()
                                .build();

                appender.add(record);
                appender.close();

                org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(martTable.spec())
                                .withPath(outputFile.location())
                                .withFileSizeInBytes(appender.length())
                                .withRecordCount(appender.metrics().recordCount())
                                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                                .build();

                martTable.newAppend().appendFile(dataFile).commit();

                // 3. Verify
                Table loadedMart = catalog.loadTable(martId);
                assertThat(loadedMart.spec().fields()).hasSize(1);
                assertThat(loadedMart.spec().fields().get(0).name()).isEqualTo("activity");

                CloseableIterable<Record> results = IcebergGenerics.read(loadedMart).build();
                assertThat(results).hasSize(1);
        }

        @Test
        void testPartitionEvolution() throws Exception {
                // 1. Initial State: Validated by previous tests (Schema has identity(purpose),
                // etc.)
                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");

                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                                new org.apache.hadoop.conf.Configuration());
                Table table = catalog.loadTable(TableIdentifier.of("gold", "telemetry"));

                // 2. Evolve Partition Spec: Add 'trace_id' as a partition field
                // "Dynamic Partition Evolution" allows changing the spec without rewriting old
                // data.
                table.updateSpec()
                                .addField("trace_id")
                                .commit();

                Table evolvedTable = catalog.loadTable(TableIdentifier.of("gold", "telemetry"));
                assertThat(evolvedTable.spec().fields()).hasSize(5); // 4 original + 1 new
                assertThat(evolvedTable.spec().fields().get(4).name()).isEqualTo("trace_id");

                // 3. Write Data with New Spec
                GenericRecord record = GenericRecord.create(evolvedTable.schema());
                String traceId = UUID.randomUUID().toString();
                record.setField("trace_id", traceId);
                record.setField("athlete_id", "athlete_evolution");
                record.setField("timestamp", java.time.OffsetDateTime.now());
                record.setField("purpose", "evolution_test");
                record.setField("activity_type", "testing");
                record.setField("data", "{}");

                // We must manually construct the partition key for the NEW spec
                // Spec: purpose, activity_type, event_id, day(timestamp), trace_id
                org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(evolvedTable.spec(),
                                evolvedTable.schema());
                partitionKey.set(0, "evolution_test"); // purpose
                partitionKey.set(1, "testing"); // activity
                partitionKey.set(2, null); // event_id (optional)
                partitionKey.set(3, (int) (Instant.now().toEpochMilli() / (1000 * 60 * 60 * 24))); // day
                partitionKey.set(4, traceId); // trace_id (NEW)

                String filename = "data/evolution/" + UUID.randomUUID() + ".parquet";
                OutputFile outputFile = evolvedTable.io().newOutputFile(evolvedTable.location() + "/" + filename);

                org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                                .schema(evolvedTable.schema())
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite()
                                .build();
                appender.add(record);
                appender.close();

                org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(evolvedTable.spec())
                                .withPath(outputFile.location())
                                .withFileSizeInBytes(appender.length())
                                .withPartition(partitionKey)
                                .withRecordCount(1)
                                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                                .build();

                evolvedTable.newAppend().appendFile(dataFile).commit();

                // 4. Verify Read
                CloseableIterable<Record> results = IcebergGenerics.read(evolvedTable)
                                .where(org.apache.iceberg.expressions.Expressions.equal("trace_id", traceId))
                                .build();
                assertThat(results).hasSize(1);
        }

        @Test
        void testCustomHierarchy() throws Exception {
                // Defines a DIFFERENT hierarchical strategy: Region -> Department -> Data
                // This demonstrates how to test a change in strategy.

                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");

                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                                new org.apache.hadoop.conf.Configuration());

                TableIdentifier customId = TableIdentifier.of("gold", "global_corp");
                if (catalog.tableExists(customId))
                        catalog.dropTable(customId);

                // 1. Define Custom Hierarchy: Region (Identity) -> Department (Identity)
                org.apache.iceberg.types.Types.NestedField[] fields = {
                                org.apache.iceberg.types.Types.NestedField.required(1, "region",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(2, "department",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(3, "data",
                                                org.apache.iceberg.types.Types.StringType.get())
                };
                Schema customSchema = new Schema(fields);

                org.apache.iceberg.PartitionSpec customSpec = org.apache.iceberg.PartitionSpec.builderFor(customSchema)
                                .identity("region") // Level 1
                                .identity("department") // Level 2
                                .build();

                catalog.createTable(customId, customSchema, customSpec);
                Table table = catalog.loadTable(customId);

                // 2. Write Data
                GenericRecord record = GenericRecord.create(customSchema);
                record.setField("region", "EMEA");
                record.setField("department", "Sales");
                record.setField("data", "Q1 Reports");

                String filename = "data/global/" + UUID.randomUUID() + ".parquet";
                OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + filename);

                org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                                .schema(customSchema)
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite()
                                .build();
                appender.add(record);
                appender.close();

                org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(customSpec)
                                .withPath(outputFile.location())
                                .withFileSizeInBytes(appender.length())
                                .withRecordCount(1)
                                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                                .withPartitionPath("region=EMEA/department=Sales") // Explicit check of hierarchy path
                                .build();

                table.newAppend().appendFile(dataFile).commit();

                // 3. Verify Hierarchy
                // Verify the Spec defines the hierarchy
                assertThat(table.spec().fields()).hasSize(2); // region, department
                assertThat(table.spec().fields().get(0).name()).isEqualTo("region");
                assertThat(table.spec().fields().get(1).name()).isEqualTo("department");

                // Verify the DataFile knows which partition it belongs to
                assertThat(dataFile.partition().get(0, String.class)).isEqualTo("EMEA");
                assertThat(dataFile.partition().get(1, String.class)).isEqualTo("Sales");
        }

        @Test
        void testReindexing() throws Exception {
                // 1. Initial State: Partition by 'department'
                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");

                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo", properties,
                                new org.apache.hadoop.conf.Configuration());

                TableIdentifier id = TableIdentifier.of("gold", "reindex_test");
                if (catalog.tableExists(id))
                        catalog.dropTable(id);

                org.apache.iceberg.types.Types.NestedField[] fields = {
                                org.apache.iceberg.types.Types.NestedField.required(1, "department",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(2, "region",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(3, "data",
                                                org.apache.iceberg.types.Types.StringType.get())
                };
                Schema schema = new Schema(fields);
                org.apache.iceberg.PartitionSpec specA = org.apache.iceberg.PartitionSpec.builderFor(schema)
                                .identity("department")
                                .build();

                catalog.createTable(id, schema, specA);
                Table table = catalog.loadTable(id);

                // 2. Write Data (Department=Sales, Region=US) -> Path: department=Sales/...
                GenericRecord record = GenericRecord.create(schema);
                record.setField("department", "Sales");
                record.setField("region", "US");
                record.setField("data", "v1");

                String filename = "data/reindex/" + UUID.randomUUID() + ".parquet";
                OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + filename);

                org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                                .schema(schema)
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite()
                                .build();
                appender.add(record);
                appender.close();

                org.apache.iceberg.DataFile dataFile = org.apache.iceberg.DataFiles.builder(specA)
                                .withPath(outputFile.location())
                                .withFileSizeInBytes(appender.length())
                                .withRecordCount(1)
                                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                                .withPartitionPath("department=Sales")
                                .build();
                table.newAppend().appendFile(dataFile).commit();

                // 3. Evolve Spec: Add 'region' -> (department, region)
                table.updateSpec()
                                .addField("region")
                                .commit();

                // 4. Trigger Re-index
                strategy.reindexData("reindex_test");

                // 5. Verify New Files have new structure
                Table reloaded = catalog.loadTable(id);
                try (CloseableIterable<FileScanTask> tasks = reloaded.newScan().planFiles()) {
                        boolean foundNewPartition = false;
                        for (FileScanTask task : tasks) {
                                // The logical partition should now include region
                                String partitionPath = task.file().partition().toString();
                                // PartitionData toString() implies dictionary order values?
                                // Actually better to check raw partition data
                                String dept = task.file().partition().get(0, String.class);
                                String reg = task.file().partition().get(1, String.class);

                                assertThat(dept).isEqualTo("Sales");
                                assertThat(reg).isEqualTo("US");
                                foundNewPartition = true;
                        }
                        assertThat(foundNewPartition).isTrue();
                }
        }
}
