package org.example.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class BaseTestcontainersTest {

    static final KafkaContainer kafka;
    static final GenericContainer<?> redis;
    static final GenericContainer<?> minio;

    static {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
        kafka.start();

        redis = new GenericContainer<>(DockerImageName.parse("redis:alpine"))
                .withExposedPorts(6379);
        redis.start();

        minio = new GenericContainer<>(DockerImageName.parse("minio/minio"))
                .withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", "admin")
                .withEnv("MINIO_ROOT_PASSWORD", "password")
                .withCommand("server /data")
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
