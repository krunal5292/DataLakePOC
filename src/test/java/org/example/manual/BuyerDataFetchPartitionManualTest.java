package org.example.manual;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.consent.model.ComplexConsentRule;
import org.example.query.model.BuyerDataResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Manual test for Buyer Data Fetch API using Partition strategy.
 * 
 * This test uses local infrastructure instead of Testcontainers:
 * - Kafka: localhost:9093
 * - Redis: localhost:6379
 * - MinIO: localhost:9000
 * 
 * Run docker-compose up before executing this test.
 */
@Profile("manual")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "consent.enforcement.strategy=partition",
        // Local Infrastructure Config
        "spring.kafka.bootstrap-servers=localhost:9093",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "minio.url=http://localhost:9000",
        "minio.access-key=admin",
        "minio.secret-key=password",
        // Skip backlog
        "gold.group.id=gold-group-manual-buyer-partition",
        "spring.kafka.consumer.auto-offset-reset=latest"
})
public class BuyerDataFetchPartitionManualTest {

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
        // Allow Kafka consumer to join group
        Thread.sleep(5000);

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

        // 4. Wait for gold layer processing using strategy.verifyDataExists
        System.out.println("⏳ Waiting for gold layer processing...");

        await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).untilAsserted(() -> {
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

        System.out.println("✅ Retrieved " + buyerResponse.getTotalRecords() + " records for " + consentedPurpose);

        // 6. Verify data content
        List<String> athleteIdsInResponse = buyerResponse.getData().stream()
                .map(item -> item.getAthleteId())
                .distinct()
                .collect(Collectors.toList());

        System.out.println("📊 Athletes in response: " + athleteIdsInResponse);

        boolean containsTestAthlete = buyerResponse.getData().stream()
                .anyMatch(item -> athleteId.equals(item.getAthleteId()));

        assertThat(containsTestAthlete).as("Response should contain test athlete's data").isTrue();

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

        System.out.println("✅ Retrieved " + batchBuyerResponse.getTotalRecords() + " records for batch request");
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
