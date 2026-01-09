package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
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
import org.awaitility.Awaitility;
import org.example.consent.model.ComplexConsentRule;
import org.example.processing.strategy.IcebergPhysicalPartitionStrategy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
                "consent.enforcement.strategy=iceberg",
                "spring.kafka.consumer.group-id.bronze=bronze-group-iceberg-test",
                "spring.kafka.consumer.group-id.silver=silver-group-iceberg-test",
                "spring.kafka.consumer.group-id.gold=gold-group-iceberg-test",
                "iceberg.table.name=telemetry_integration_test"
}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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

        @Autowired
        private ResourceLoader resourceLoader;

        @Autowired
        private org.springframework.core.env.Environment environment;

        @Autowired
        private TestRestTemplate restTemplate;

        @Test
        void testFanOutCreatesCorrectPartitions() throws Exception {
                // 1. Setup Data using Resources
                Resource ruleResource = resourceLoader.getResource("classpath:consent_rule.json");
                Resource dataResource = resourceLoader.getResource("classpath:raw_data.json");

                List<ComplexConsentRule> rules = objectMapper.readValue(ruleResource.getInputStream(),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });
                ComplexConsentRule rule = rules.get(0); // Use first rule
                String athleteId = rule.getUserId();

                List<Map<String, Object>> dataList = objectMapper.readValue(dataResource.getInputStream(),
                                new TypeReference<List<Map<String, Object>>>() {
                                });
                Map<String, Object> payload = dataList.get(0); // Use first record

                // Ensure payload has athleteId matching the rule
                payload.put("athlete_id", athleteId);
                // Ensure trace_id is present if expected by controller, though controller
                // usually keys it.
                // ingest endpoint might expect specific keys.
                String traceId = UUID.randomUUID().toString();
                payload.put("trace_id", traceId);

                // Consent Rule is loaded from file
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // 2. Ingest Data via API (E2E)
                ResponseEntity<Map> response = restTemplate.postForEntity("/api/ingest", payload, Map.class);
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

                // 3. Verify Iceberg Data (Waiter)
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

                // Wait for table and data
                Awaitility.await().atMost(90, TimeUnit.SECONDS).until(() -> {
                        try {
                                if (!catalog.tableExists(TableIdentifier.of("gold", "telemetry_integration_test"))) {
                                        return false;
                                }
                                Table table = catalog
                                                .loadTable(TableIdentifier.of("gold", "telemetry_integration_test"));
                                CloseableIterable<Record> records = IcebergGenerics.read(table)
                                                .where(org.apache.iceberg.expressions.Expressions.equal("purpose",
                                                                "sportsAndPerformance"))
                                                .build();

                                // Check if OUR athlete's data exists
                                for (Record r : records) {
                                        if (r.getField("athlete_id").toString().equals(athleteId)) {
                                                return true;
                                        }
                                }
                                return false;
                        } catch (Exception e) {
                                return false;
                        }
                });

                Table table = catalog.loadTable(TableIdentifier.of("gold", "telemetry_integration_test"));

                // Verify Research (First rule value is 'research')
                CloseableIterable<Record> researchRecords = IcebergGenerics.read(table)
                                .where(org.apache.iceberg.expressions.Expressions.equal("purpose", "research"))
                                .build();

                Record r1 = null;
                for (Record r : researchRecords) {
                        if (r.getField("athlete_id").toString().equals(athleteId)) {
                                r1 = r;
                                break;
                        }
                }
                assertThat(r1).isNotNull();

                assertThat(r1.getField("athlete_id")).isEqualTo(athleteId);
                // As discovered, activity_type defaults to unknown if missing from
                // raw_data.json
                assertThat(r1.getField("activity_type")).isEqualTo("unknown");

                // Verify SportsAndPerformance (Also present in first rule)
                CloseableIterable<Record> sportsRecords = IcebergGenerics.read(table)
                                .where(org.apache.iceberg.expressions.Expressions.equal("purpose",
                                                "sportsAndPerformance"))
                                .build();

                long matchCount = java.util.stream.StreamSupport.stream(sportsRecords.spliterator(), false)
                                .filter(r -> r.getField("athlete_id").toString().equals(athleteId))
                                .count();
                assertThat(matchCount).isEqualTo(1);

        }

        @Test
        void testRevocationRemovesData() throws Exception {
                // Use a unique purpose to avoid collision with other tests (e.g. testFanOut
                // uses 'research')
                String uniquePurpose = "research_revoke_test";
                String athleteId = "athlete_revoke_" + UUID.randomUUID(); // Unique athlete
                String traceId = UUID.randomUUID().toString();

                // Consent to Research
                ComplexConsentRule rule = new ComplexConsentRule();
                rule.setUserId(athleteId);
                rule.setStatus("ACTIVE");
                ComplexConsentRule.Dimensions dims = new ComplexConsentRule.Dimensions();
                dims.setPurpose(new ComplexConsentRule.DimensionDetail("specific",
                                List.of(new ComplexConsentRule.ValueDetail("1", uniquePurpose, "Research Test")),
                                null));
                dims.setEvents(new ComplexConsentRule.DimensionDetail("any", null, null));
                rule.setDimensions(dims);
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // Enriched Data
                Map<String, Object> payload = new HashMap<>();
                payload.put("activity_type", "cycling");
                payload.put("athlete_id", athleteId); // Required by controller
                payload.put("purpose", uniquePurpose); // Just in case, though strategy gets it from Rule

                // 2. Ingest Data via API (E2E)
                ResponseEntity<Map> response = restTemplate.postForEntity("/api/ingest", payload, Map.class);
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

                // Wait for data to exist (E2E async processing)
                // Add logging to debug
                try {
                        Awaitility.await().atMost(90, TimeUnit.SECONDS)
                                        .until(() -> {
                                                boolean exists = strategy.verifyDataExists(athleteId, uniquePurpose);
                                                if (!exists) {
                                                        System.out.println("Waiting for data... Consumer group: "
                                                                        + environment.getProperty(
                                                                                        "spring.kafka.consumer.group-id.gold"));
                                                }
                                                return exists;
                                        });
                } catch (org.awaitility.core.ConditionTimeoutException e) {
                        // Fail hard but maybe dump something?
                        System.err.println("TIMEOUT WAITING FOR INGESTION: " + athleteId);
                        throw e;
                }

                assertThat(strategy.verifyDataExists(athleteId, uniquePurpose)).isTrue();

                // 2. Revoke result
                // 3. Revoke with Retry (OCC)
                Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
                        try {
                                strategy.handleRevocation(athleteId, uniquePurpose);
                                // Robustness: check if data is actually gone.
                                // Taking into account potential concurrent writes or missed files.
                                boolean stillExists = strategy.verifyDataExists(athleteId, uniquePurpose);
                                if (stillExists) {
                                        System.out.println("Data still matches after revocation. Retrying...");
                                        return false; // Retry loop
                                }
                                return true; // Success
                        } catch (org.apache.iceberg.exceptions.CommitFailedException e) {
                                System.out.println("Commit failed (OCC). Retrying...");
                                return false; // Retry
                        }
                });

                // 3. Verify Gone
                assertThat(strategy.verifyDataExists(athleteId, uniquePurpose)).isFalse();
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
                Table table = catalog.loadTable(TableIdentifier.of("gold", "telemetry_integration_test"));

                // 2. Evolve Partition Spec: Add 'trace_id' as a partition field
                // "Dynamic Partition Evolution" allows changing the spec without rewriting old
                // data.
                table.updateSpec()
                                .addField("trace_id")
                                .commit();

                Table evolvedTable = catalog.loadTable(TableIdentifier.of("gold", "telemetry_integration_test"));
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
