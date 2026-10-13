package com.example.chat.realtime;

import java.util.function.Consumer;

import com.example.chat.common.Json;
import com.example.chat.config.ChatMetrics;
import com.example.chat.config.InstanceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * 인스턴스 간 팬아웃. Redis Pub/Sub 은 at-most-once 이므로 유실될 수 있다 —
 * 유실은 수신 측 gap-fill(EventDelivery) 과 클라이언트 resume 이 DB 원장 기준으로 복구한다.
 */
@Component
public class RedisEventBus {

    public static final String CHANNEL = "chat:bus";
    private static final Logger log = LoggerFactory.getLogger(RedisEventBus.class);

    private final StringRedisTemplate redis;
    private final RedisMessageListenerContainer container;
    private final Json json;
    private final InstanceId instanceId;
    private final ChatMetrics metrics;

    public RedisEventBus(StringRedisTemplate redis, RedisMessageListenerContainer container, Json json,
                         InstanceId instanceId, ChatMetrics metrics) {
        this.redis = redis;
        this.container = container;
        this.json = json;
        this.instanceId = instanceId;
        this.metrics = metrics;
    }

    public void publish(BusMessage message) {
        try {
            redis.convertAndSend(CHANNEL, json.write(message));
        } catch (Exception e) {
            metrics.busPublishFailed();
            log.warn("bus publish failed kind={} session={}: {}", message.kind(), message.sessionId(), e.getMessage());
        }
    }

    /** 자기 자신이 보낸 메시지는 이미 로컬 전달했으므로 걸러낸다. */
    public void subscribe(Consumer<BusMessage> handler) {
        MessageListener listener = (Message message, byte[] pattern) -> {
            try {
                BusMessage m = json.read(new String(message.getBody()), BusMessage.class);
                if (!instanceId.value().equals(m.origin())) {
                    handler.accept(m);
                }
            } catch (Exception e) {
                log.warn("bus message handling failed: {}", e.getMessage());
            }
        };
        container.addMessageListener(listener, new ChannelTopic(CHANNEL));
    }
}
