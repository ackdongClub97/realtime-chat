package com.example.chat.it;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 실제 PostgreSQL / Redis 컨테이너로 실행하는 통합 테스트 베이스. 컨테이너는 테스트 클래스 간 공유한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "chat.instance-id=it",
        "chat.snapshot.interval=10",
        "chat.outbox.poll-interval-ms=50",
        "chat.outbox.base-backoff-ms=20",
        "chat.outbox.max-attempts=3",
        "chat.chaos.enabled=true"
})
abstract class IntegrationTestBase {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static {
        POSTGRES.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.data.redis.host", REDIS::getHost);
        r.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
}
