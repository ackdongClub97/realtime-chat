package com.example.chat.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class RedisConfig {

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory factory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        // 기본값(SimpleAsyncTaskExecutor)은 메시지마다 스레드를 만든다 → 부하 시 스레드 폭증.
        // 수신 순서를 유지하도록 단일 스레드로 역직렬화만 하고, 실제 전송은 세션별 레인(SessionStripedExecutor)에 넘긴다.
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(100_000);
        executor.setThreadNamePrefix("redis-sub-");
        executor.initialize();
        container.setTaskExecutor(executor);
        // Redis 장애 후 복구되면 자동 재구독
        container.setRecoveryInterval(2000L);
        return container;
    }
}
