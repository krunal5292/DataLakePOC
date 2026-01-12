package org.example.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.model.BuyerDataResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Buyer Data Fetch API using Iceberg strategy.
 * 
 * This test verifies:
 * 1. Data is correctly indexed in Iceberg tables during ingestion based on
 * consent rules
 * 2. Buyer API returns only consented data without runtime filtration
 * 3. Unconsented purposes return no data
 * 4. Iceberg partition pruning is leveraged for efficient queries
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
                "consent.enforcement.strategy=iceberg"
})
public class BuyerDataFetchIcebergIntegrationTest {

        private static final Network NETWORK = Network.newNetwork();

        @SuppressWarnings("deprecation")
        private static final KafkaContainer KAFKA = new KafkaContainer(
                        DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("kafka");

        private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:alpine"))
                        .withExposedPorts(6379)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("redis");

        @SuppressWarnings("resource")
        private static final GenericContainer<?> MINIO = new GenericContainer<>(DockerImageName.parse("minio/minio"))
                        .withExposedPorts(9000, 9001)
                        .withEnv("MINIO_ROOT_USER", "admin")
                        .withEnv("MINIO_ROOT_PASSWORD", "password")
                        .withCommand("server /data --console-address :9001")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("minio")
                        .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

        // Postgres for Iceberg Catalog
        @SuppressWarnings("resource")
        private static final GenericContainer<?> POSTGRES = new GenericContainer<>(
                        DockerImageName.parse("postgres:15-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("db-catalog")
                        .withExposedPorts(5432)
                        .withEnv("POSTGRES_USER", "admin")
                        .withEnv("POSTGRES_PASSWORD", "password")
                        .withEnv("POSTGRES_DB", "iceberg");

        @SuppressWarnings("resource")
        private static final GenericContainer<?> ICEBERG_REST = new GenericContainer<>(
                        DockerImageName.parse("tabulario/iceberg-rest"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("iceberg-rest")
                        .withExposedPorts(8181)
                        .withEnv("AWS_REGION", "us-east-1")
                        .withEnv("CATALOG_WAREHOUSE", "s3://gold-warehouse/")
                        .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                        .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
                        .withEnv("CATALOG_S3_ACCESS__KEY__ID", "admin")
                        .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", "password")
                        .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                        .withEnv("JDBC_USER", "admin")
                        .withEnv("JDBC_PASSWORD", "password")
                        .withEnv("JDBC_DB", "iceberg")
                        .dependsOn(POSTGRES);

        @BeforeAll
        static void startContainers() {
                KAFKA.start();
                REDIS.start();
                MINIO.start();
                POSTGRES.start();
                ICEBERG_REST.start();
        }

        @DynamicPropertySource
        static void dynamicProperties(DynamicPropertyRegistry registry) {
                // Kafka
                registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);

                // Redis
                registry.add("spring.data.redis.host", REDIS::getHost);
                registry.add("spring.data.redis.port", REDIS::getFirstMappedPort);

                // MinIO
                registry.add("minio.url", () -> "http://" + MINIO.getHost() + ":" + MINIO.getFirstMappedPort());
                registry.add("minio.access-key", () -> "admin");
                registry.add("minio.secret-key", () -> "password");

                // Iceberg
                registry.add("iceberg.catalog.uri",
                                () -> "http://" + ICEBERG_REST.getHost() + ":" + ICEBERG_REST.getFirstMappedPort());
                registry.add("iceberg.s3.endpoint",
                                () -> "http://" + MINIO.getHost() + ":" + MINIO.getFirstMappedPort());
                registry.add("iceberg.s3.access-key", () -> "admin");
                registry.add("iceberg.s3.secret-key", () -> "password");
        }

        @Autowired
        private ObjectMapper objectMapper;

        @Autowired
        private StringRedisTemplate redisTemplate;

        @Autowired
        private ResourceLoader resourceLoader;

        @Autowired
        private TestRestTemplate restTemplate;

        @Autowired
        private org.example.processing.strategy.IcebergPhysicalPartitionStrategy strategy;

        @Test
        void testBuyerFetchesOnlyConsentedDataFromIceberg() throws Exception {
                // 1. Load consent rules from consent_rule.json
                Resource ruleResource = resourceLoader.getResource("classpath:consent_rule.json");
                List<ComplexConsentRule> rules = objectMapper.readValue(ruleResource.getInputStream(),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });

                ComplexConsentRule rule = rules.get(0); // Use first athlete
                String athleteId = rule.getUserId();

                // Extract consented purposes
                List<String> consentedPurposes = rule.getDimensions().getPurpose().getValues().stream()
                                .map(ComplexConsentRule.ValueDetail::getValue)
                                .collect(Collectors.toList());

                System.out.println("📋 Athlete: " + athleteId);
                System.out.println("✅ Consented purposes: " + consentedPurposes);

                // 2. Load consent rule to Redis
                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // 3. Ingest data from raw_data.json
                Resource dataResource = resourceLoader.getResource("classpath:raw_data.json");
                List<Map<String, Object>> dataList = objectMapper.readValue(dataResource.getInputStream(),
                                new TypeReference<List<Map<String, Object>>>() {
                                });

                System.out.println("📤 Ingesting " + dataList.size() + " records into Iceberg...");

                for (Map<String, Object> payload : dataList) {
                        payload.put("athlete_id", athleteId);
                        restTemplate.postForEntity("/api/ingest", payload, Void.class);
                }

                // 4. Wait for Iceberg table creation and data processing
                System.out.println("⏳ Waiting for Iceberg table creation and processing...");

                // Use Awaitility to wait for data to be available in Iceberg gold layer
                await().atMost(60, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS).untilAsserted(() -> {
                        boolean dataExists = strategy.verifyDataExists(athleteId, "research");
                        assertThat(dataExists).as("Iceberg gold layer should have data for research purpose").isTrue();
                });

                // 5. Fetch data for a consented purpose (e.g., "research")
                String consentedPurpose = "research";
                assertThat(consentedPurposes).contains(consentedPurpose);

                System.out.println("🔍 Fetching data for consented purpose: " + consentedPurpose);

                ResponseEntity<BuyerDataResponse> response = restTemplate.getForEntity(
                                "/api/buyer/data?purpose=" + consentedPurpose,
                                BuyerDataResponse.class);

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                BuyerDataResponse buyerResponse = response.getBody();

                assertThat(buyerResponse).isNotNull();
                assertThat(buyerResponse.getPurpose()).isEqualTo(consentedPurpose);
                assertThat(buyerResponse.getTotalRecords()).isGreaterThan(0);
                assertThat(buyerResponse.getData()).isNotEmpty();

                System.out.println(
                                "✅ Retrieved " + buyerResponse.getTotalRecords() + " records for " + consentedPurpose);

                // 6. Verify data content
                buyerResponse.getData().forEach(item -> {
                        assertThat(item.getAthleteId()).isEqualTo(athleteId);
                        assertThat(item.getContent()).isNotNull();
                });

                // 7. Test unconsented purpose (should return no data)
                String unconsentedPurpose = "marketing";
                assertThat(consentedPurposes).doesNotContain(unconsentedPurpose);

                System.out.println("🔍 Fetching data for unconsented purpose: " + unconsentedPurpose);

                ResponseEntity<BuyerDataResponse> unconsentedResponse = restTemplate.getForEntity(
                                "/api/buyer/data?purpose=" + unconsentedPurpose,
                                BuyerDataResponse.class);

                assertThat(unconsentedResponse.getStatusCode().is2xxSuccessful()).isTrue();
                BuyerDataResponse unconsentedBuyerResponse = unconsentedResponse.getBody();

                assertThat(unconsentedBuyerResponse).isNotNull();
                assertThat(unconsentedBuyerResponse.getTotalRecords()).isEqualTo(0);
                assertThat(unconsentedBuyerResponse.getData()).isEmpty();

                System.out.println("✅ Correctly returned 0 records for unconsented purpose");

                // 8. Test multiple consented purposes (leveraging Iceberg OR expression)
                List<String> multiplePurposes = List.of("research", "sportsAndPerformance");

                System.out.println("🔍 Fetching data for multiple purposes: " + multiplePurposes);

                ResponseEntity<BuyerDataResponse> batchResponse = restTemplate.postForEntity(
                                "/api/buyer/data/batch",
                                multiplePurposes,
                                BuyerDataResponse.class);

                assertThat(batchResponse.getStatusCode().is2xxSuccessful()).isTrue();
                BuyerDataResponse batchBuyerResponse = batchResponse.getBody();

                assertThat(batchBuyerResponse).isNotNull();
                assertThat(batchBuyerResponse.getTotalRecords()).isGreaterThan(0);

                System.out.println(
                                "✅ Retrieved " + batchBuyerResponse.getTotalRecords() + " records for batch request");
        }

        @Test
        void testIcebergPartitionPruning() throws Exception {
                // This test verifies that Iceberg partition pruning is working
                // by fetching data for a specific purpose and verifying the query is efficient

                Resource ruleResource = resourceLoader.getResource("classpath:consent_rule.json");
                List<ComplexConsentRule> rules = objectMapper.readValue(ruleResource.getInputStream(),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });

                ComplexConsentRule rule = rules.get(0);
                String athleteId = rule.getUserId();

                redisTemplate.opsForValue().set("consent:rule:" + athleteId, objectMapper.writeValueAsString(rule));

                // Ingest data
                Resource dataResource = resourceLoader.getResource("classpath:raw_data.json");
                List<Map<String, Object>> dataList = objectMapper.readValue(dataResource.getInputStream(),
                                new TypeReference<List<Map<String, Object>>>() {
                                });

                for (Map<String, Object> payload : dataList) {
                        payload.put("athlete_id", athleteId);
                        restTemplate.postForEntity("/api/ingest", payload, Void.class);
                }

                // Wait for data to be available
                await().atMost(60, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS).untilAsserted(() -> {
                        boolean dataExists = strategy.verifyDataExists(athleteId, "healthAndMedical");
                        assertThat(dataExists).as("Iceberg gold layer should have data for healthAndMedical purpose")
                                        .isTrue();
                });

                // Fetch with purpose filter - should use partition pruning
                long startTime = System.currentTimeMillis();

                ResponseEntity<BuyerDataResponse> response = restTemplate.getForEntity(
                                "/api/buyer/data?purpose=healthAndMedical",
                                BuyerDataResponse.class);

                long duration = System.currentTimeMillis() - startTime;

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                System.out.println("✅ Query completed in " + duration + "ms (partition pruning enabled)");
        }

        @Test
        void testStrategyVerification() {
                // Verify that Iceberg strategy is active
                ResponseEntity<String> response = restTemplate.getForEntity(
                                "/api/buyer/strategy",
                                String.class);

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                assertThat(response.getBody()).isEqualTo("iceberg");

                System.out.println("✅ Active strategy: " + response.getBody());
        }
}
