package org.example.manual;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.example.consent.model.ComplexConsentRule;
import org.example.processing.strategy.IcebergPhysicalPartitionStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
                "consent.enforcement.strategy=iceberg",
                // Local Infrastructure Config
                "spring.kafka.bootstrap-servers=localhost:9093",
                "spring.data.redis.host=localhost",
                "spring.data.redis.port=6379",
                "minio.url=http://localhost:9000",
                "minio.access-key=admin",
                "minio.secret-key=password",
                "iceberg.catalog.uri=http://localhost:8181",
                "iceberg.s3.endpoint=http://localhost:9000",
                "iceberg.s3.access-key=admin",
                "iceberg.s3.secret-key=password",
                // Skip backlog
                "gold.group.id=gold-group-manual-clean",
                "spring.kafka.consumer.auto-offset-reset=latest"
})
public class IcebergStrategyManualTest {

        static {
                System.setProperty("aws.region", "us-east-1");
                System.setProperty("aws.accessKeyId", "admin");
                System.setProperty("aws.secretAccessKey", "password");
        }

        @Autowired
        private IcebergPhysicalPartitionStrategy strategy;

        @Autowired
        private ObjectMapper objectMapper;

        @Autowired
        private StringRedisTemplate redisTemplate;

        @Autowired
        private ResourceLoader resourceLoader;

        @Autowired
        private TestRestTemplate restTemplate;

        @Test
        void testFullIcebergFanOutFlow() throws Exception {
                // 1. Setup Data using Resources
                Resource ruleResource = resourceLoader.getResource("classpath:consent_rule.json");
                Resource dataResource = resourceLoader.getResource("classpath:raw_data.json");

                List<ComplexConsentRule> rules = objectMapper.readValue(ruleResource.getInputStream(),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });
                ComplexConsentRule rule = rules.get(0); // Use first rule user
                String athleteId = rule.getUserId();

                // Extract expected purposes for verification
                List<String> consentedPurposes = rule.getDimensions().getPurpose().getValues().stream()
                                .map(ComplexConsentRule.ValueDetail::getValue)
                                .collect(Collectors.toList());

                // Load Rules to Redis
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // Load Raw Data
                List<Map<String, Object>> dataList = objectMapper.readValue(dataResource.getInputStream(),
                                new TypeReference<List<Map<String, Object>>>() {
                                });

                System.out.println("📤 Ingesting " + dataList.size() + " records for athlete: " + athleteId);

                // 2. Ingest Data via REST API
                for (Map<String, Object> payload : dataList) {
                        // Ensure athlete_id matches the rule
                        payload.put("athlete_id", athleteId);
                        restTemplate.postForEntity("/api/ingest", payload, Map.class);
                }

                // 3. Await Processing (Gold Layer)
                await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
                        Table table;
                        try {
                                table = loadIcebergTable();
                        } catch (Exception e) {
                                assertThat(false).as("Iceberg table not yet created").isTrue();
                                return;
                        }

                        // Verify ALL consented purposes have data
                        for (String purpose : consentedPurposes) {
                                CloseableIterable<Record> results = IcebergGenerics.read(table)
                                                .where(Expressions.and(
                                                                Expressions.equal("purpose", purpose),
                                                                Expressions.equal("athlete_id", athleteId)))
                                                .build();
                                assertThat(results).as("Data missing for purpose: " + purpose).isNotEmpty();
                        }
                });

                // 4. Verify Iceberg Structure & Data
                System.out.println("🔍 Verifying Iceberg Table Structure and Data Content...");
                Table table = loadIcebergTable();

                // 4a. Verify Partition Spec
                PartitionSpec spec = table.spec();
                System.out.println("Current Spec: " + spec);

                assertThat(spec.fields()).hasSize(4);
                assertThat(spec.fields().get(0).name()).isEqualTo("purpose");
                assertThat(spec.fields().get(1).name()).isEqualTo("activity_type");
                assertThat(spec.fields().get(2).name()).isEqualTo("event_id");
                // Verify day transform on timestamp
                assertThat(spec.fields().get(3).transform().toString()).contains("day");

                // 4b. Verify Data for Consented Purposes
                for (String purpose : consentedPurposes) {
                        System.out.println("Checking purpose: " + purpose);
                        CloseableIterable<Record> results = IcebergGenerics.read(table)
                                        .where(Expressions.and(
                                                        Expressions.equal("purpose", purpose),
                                                        Expressions.equal("athlete_id", athleteId)))
                                        .build();

                        // We expect data to be fanned out to ALL consented purposes
                        assertThat(results).isNotEmpty();

                        // Verify content matches partition
                        for (Record r : results) {
                                assertThat(r.getField("purpose")).isEqualTo(purpose);
                        }
                }

                // 4c. Verify Negative Case (Unconsented Purpose)
                String unconsentedPurpose = "marketing";
                System.out.println("Checking negative case: " + unconsentedPurpose);
                CloseableIterable<Record> negativeResults = IcebergGenerics.read(table)
                                .where(Expressions.equal("purpose", unconsentedPurpose))
                                .build();
                assertThat(negativeResults).isEmpty();

                System.out.println("✅ All Iceberg Verifications Passed!");
        }

        @Test
        void testRevocationRemovesData() throws Exception {
                // Allow Kafka consumer to join group (since we use 'latest' and new group)
                Thread.sleep(5000);

                String athleteId = "athlete_revoke_" + UUID.randomUUID().toString().substring(0, 8);

                // 1. Consent from JSON (Same as working test)
                List<ComplexConsentRule> rules = objectMapper.readValue(
                                getClass().getClassLoader().getResourceAsStream("consent_rule.json"),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });
                ComplexConsentRule rule = rules.get(0);
                rule.setUserId(athleteId);
                rule.setStatus("ACTIVE");
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // 2. Ingest Data from JSON (Same as working test)
                List<Map<String, Object>> dataRows = objectMapper.readValue(
                                getClass().getClassLoader().getResourceAsStream("raw_data.json"),
                                new TypeReference<List<Map<String, Object>>>() {
                                });
                Map<String, Object> payload = dataRows.get(0);
                payload.put("athlete_id", athleteId);
                payload.put("timestamp", Instant.now().toString());

                System.out.println("TEST_REVOKE_ID: " + athleteId);
                org.springframework.http.ResponseEntity<Void> response = restTemplate.postForEntity("/api/ingest",
                                payload, Void.class);
                assertThat(response.getStatusCode().is2xxSuccessful()).as("Ingestion should accept data").isTrue();

                // 3. Await Data Presence (Gold Layer) for "research" (present in
                // consent_rule.json)
                await().atMost(120, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
                        boolean exists = strategy.verifyDataExists(athleteId, "research");
                        assertThat(exists).as("Data should exist for 'research' purpose").isTrue();
                });

                // 4. Revoke
                strategy.handleRevocation(athleteId, "research");

                // 5. Verify Gone
                assertThat(strategy.verifyDataExists(athleteId, "research"))
                                .as("Data should be removed after revocation").isFalse();
        }

        @Test
        void testDataMartCreation() throws Exception {
                Map<String, String> properties = getLocalCatalogProperties();

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
                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo",
                                getLocalCatalogProperties(),
                                new org.apache.hadoop.conf.Configuration());

                // Create a separate table for evolution to avoid polluting shared table
                TableIdentifier evolId = TableIdentifier.of("gold", "evolution_test");
                if (catalog.tableExists(evolId))
                        catalog.dropTable(evolId);

                // Use same schema as telemetry for realism
                Schema schema = IcebergPhysicalPartitionStrategy.SCHEMA;
                PartitionSpec spec = PartitionSpec.builderFor(schema)
                                .identity("purpose")
                                .identity("activity_type")
                                .identity("event_id")
                                .day("timestamp")
                                .build();

                catalog.createTable(evolId, schema, spec);
                Table table = catalog.loadTable(evolId);

                // 2. Evolve Partition Spec: Add 'trace_id' as a partition field
                table.updateSpec()
                                .addField("trace_id")
                                .commit();

                Table evolvedTable = catalog.loadTable(evolId);
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
                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo",
                                getLocalCatalogProperties(),
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

                assertThat(table.spec().fields()).hasSize(2); // region, department
                assertThat(table.spec().fields().get(0).name()).isEqualTo("region");
                assertThat(table.spec().fields().get(1).name()).isEqualTo("department");

                assertThat(dataFile.partition().get(0, String.class)).isEqualTo("EMEA");
                assertThat(dataFile.partition().get(1, String.class)).isEqualTo("Sales");
        }

        @Test
        void testReindexing() throws Exception {
                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo",
                                getLocalCatalogProperties(),
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

                table.updateSpec()
                                .addField("region")
                                .commit();

                strategy.reindexData("reindex_test");

                Table reloaded = catalog.loadTable(id);
                assertThat(reloaded.spec().fields()).hasSize(2); // department, region
                // Verify data is still readable
                CloseableIterable<Record> results = IcebergGenerics.read(reloaded).build();
                assertThat(results).hasSize(1);
                Record readRecord = results.iterator().next();
                assertThat(readRecord.getField("department")).isEqualTo("Sales");
                assertThat(readRecord.getField("region")).isEqualTo("US");
        }

        private Table loadIcebergTable() {
                Catalog catalog = CatalogUtil.loadCatalog("org.apache.iceberg.rest.RESTCatalog", "demo",
                                getLocalCatalogProperties(),
                                new org.apache.hadoop.conf.Configuration());

                return catalog.loadTable(TableIdentifier.of("gold", "telemetry"));
        }

        private Map<String, String> getLocalCatalogProperties() {
                Map<String, String> properties = new HashMap<>();
                properties.put("uri", "http://localhost:8181");
                properties.put("warehouse", "s3://gold-warehouse/");
                properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("s3.endpoint", "http://localhost:9000");
                properties.put("s3.access-key-id", "admin");
                properties.put("s3.secret-access-key", "password");
                properties.put("s3.path-style-access", "true");
                return properties;
        }
}
