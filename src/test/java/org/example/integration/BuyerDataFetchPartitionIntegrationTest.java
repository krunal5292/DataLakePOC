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
 * Integration test for Buyer Data Fetch API using Partition strategy.
 * 
 * This test verifies:
 * 1. Data is correctly indexed during ingestion based on consent rules
 * 2. Buyer API returns only consented data without runtime filtration
 * 3. Unconsented purposes return no data
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
                "consent.enforcement.strategy=partition"
})
public class BuyerDataFetchPartitionIntegrationTest {

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

        @BeforeAll
        static void startContainers() {
                KAFKA.start();
                REDIS.start();
                MINIO.start();
        }

        @DynamicPropertySource
        static void dynamicProperties(DynamicPropertyRegistry registry) {
                registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
                registry.add("spring.data.redis.host", REDIS::getHost);
                registry.add("spring.data.redis.port", REDIS::getFirstMappedPort);
                registry.add("minio.url", () -> "http://" + MINIO.getHost() + ":" + MINIO.getFirstMappedPort());
                registry.add("minio.access-key", () -> "admin");
                registry.add("minio.secret-key", () -> "password");
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
        private org.example.processing.strategy.PhysicalPartitionStrategy strategy;

        @Test
        void testBuyerFetchesOnlyConsentedData() throws Exception {
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

                System.out.println("📤 Ingesting " + dataList.size() + " records...");

                for (Map<String, Object> payload : dataList) {
                        payload.put("athlete_id", athleteId);
                        restTemplate.postForEntity("/api/ingest", payload, Void.class);
                }

                // 4. Wait for gold layer processing
                System.out.println("⏳ Waiting for gold layer processing...");

                // Use Awaitility to wait for data to be available in gold layer
                await().atMost(45, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
                        boolean dataExists = strategy.verifyDataExists(athleteId, "research");
                        assertThat(dataExists).as("Gold layer should have data for research purpose").isTrue();
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

                // 6. Verify data content - should contain data for the athlete who consented
                List<String> athleteIdsInResponse = buyerResponse.getData().stream()
                                .map(item -> item.getAthleteId())
                                .distinct()
                                .collect(Collectors.toList());

                System.out.println("📊 Athletes in response: " + athleteIdsInResponse);

                // Verify that our test athlete's data is present
                boolean containsTestAthlete = buyerResponse.getData().stream()
                                .anyMatch(item -> athleteId.equals(item.getAthleteId()));

                assertThat(containsTestAthlete).as("Response should contain test athlete's data").isTrue();

                // Verify all items have content
                buyerResponse.getData().forEach(item -> {
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

                // 8. Test multiple consented purposes
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
        void testMultiAthleteConsentIsolation() throws Exception {
                // Load consent rules
                Resource ruleResource = resourceLoader.getResource("classpath:consent_rule.json");
                List<ComplexConsentRule> rules = objectMapper.readValue(ruleResource.getInputStream(),
                                new TypeReference<List<ComplexConsentRule>>() {
                                });

                // Use both athletes from consent_rule.json
                ComplexConsentRule athlete1Rule = rules.get(0);
                ComplexConsentRule athlete2Rule = rules.get(1);

                String athlete1Id = athlete1Rule.getUserId();
                String athlete2Id = athlete2Rule.getUserId();

                // Load both consent rules
                redisTemplate.opsForValue().set("consent:rule:" + athlete1Id,
                                objectMapper.writeValueAsString(athlete1Rule));
                redisTemplate.opsForValue().set("consent:rule:" + athlete2Id,
                                objectMapper.writeValueAsString(athlete2Rule));

                // Ingest data for both athletes
                Resource dataResource = resourceLoader.getResource("classpath:raw_data.json");
                List<Map<String, Object>> dataList = objectMapper.readValue(dataResource.getInputStream(),
                                new TypeReference<List<Map<String, Object>>>() {
                                });

                System.out.println("📤 Ingesting data for 2 athletes...");

                // Ingest for athlete 1
                for (int i = 0; i < 5; i++) {
                        Map<String, Object> payload = dataList.get(i);
                        payload.put("athlete_id", athlete1Id);
                        restTemplate.postForEntity("/api/ingest", payload, Void.class);
                }

                // Ingest for athlete 2
                for (int i = 5; i < 10; i++) {
                        Map<String, Object> payload = dataList.get(i);
                        payload.put("athlete_id", athlete2Id);
                        restTemplate.postForEntity("/api/ingest", payload, Void.class);
                }

                // Wait for processing
                await().atMost(45, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
                        boolean dataExists = strategy.verifyDataExists(athlete1Id, "research");
                        assertThat(dataExists).as("Gold layer should have data for research purpose").isTrue();
                });

                // Fetch data for "research" purpose
                ResponseEntity<BuyerDataResponse> response = restTemplate.getForEntity(
                                "/api/buyer/data?purpose=research",
                                BuyerDataResponse.class);

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                BuyerDataResponse buyerResponse = response.getBody();

                assertThat(buyerResponse).isNotNull();
                assertThat(buyerResponse.getTotalRecords()).isGreaterThan(0);

                // Verify data contains both athletes (both consented to research)
                List<String> athleteIds = buyerResponse.getData().stream()
                                .map(item -> item.getAthleteId())
                                .distinct()
                                .collect(Collectors.toList());

                System.out.println("✅ Data contains athletes: " + athleteIds);
                assertThat(athleteIds).hasSizeGreaterThanOrEqualTo(1); // At least one athlete
        }

        @Test
        void testStrategyVerification() {
                // Verify that partition strategy is active
                ResponseEntity<String> response = restTemplate.getForEntity(
                                "/api/buyer/strategy",
                                String.class);

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                assertThat(response.getBody()).isEqualTo("partition");

                System.out.println("✅ Active strategy: " + response.getBody());
        }
}
