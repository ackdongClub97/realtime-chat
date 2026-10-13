package com.example.chat.config;

import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 이 프로세스(인스턴스)의 식별자. presence 소유권, outbox 리스, 장애 감지(sweeper)에 사용한다.
 */
@Component
public class InstanceId {

    private final String value;

    public InstanceId(ChatProperties props) {
        this.value = StringUtils.hasText(props.instanceId())
                ? props.instanceId()
                : "local-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
