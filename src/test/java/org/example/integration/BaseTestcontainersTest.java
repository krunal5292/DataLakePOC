package org.example.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class BaseTestcontainersTest {

    static final Network NETWORK = Network.newNetwork();

    static final KafkaContainer kafka;
    static final GenericContainer<?> redis;
    static final GenericContainer<?> minio;

    static {
        System.setProperty("aws.region", "us-east-1");
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka");
        kafka.start();

        redis = new GenericContainer<>(DockerImageName.parse("redis:alpine"))
                .withExposedPorts(6379)
                .withNetwork(NETWORK)
                .withNetworkAliases("redis");
        redis.start();

        minio = new GenericContainer<>(DockerImageName.parse("minio/minio"))
                .withExposedPorts(9000, 9001)
                .withEnv("MINIO_ROOT_USER", "admin")
                .withEnv("MINIO_ROOT_PASSWORD", "password")
                .withCommand("server /data --console-address :9001")
                .withNetwork(NETWORK)
                .withNetworkAliases("minio")
                .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));
        minio.start();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
        registry.add("minio.url", () -> "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
        registry.add("minio.access-key", () -> "admin");
        registry.add("minio.secret-key", () -> "password");
    }
}
