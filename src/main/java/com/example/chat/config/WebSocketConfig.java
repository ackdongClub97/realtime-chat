package com.example.chat.config;

import com.example.chat.realtime.ChatWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final ChatWebSocketHandler handler;

    public WebSocketConfig(ChatWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // 인증은 과제 범위 밖(비목표): userId 를 쿼리로 받는다. 운영에서는 핸드셰이크 시 토큰 검증으로 대체.
        registry.addHandler(handler, "/ws").setAllowedOriginPatterns("*");
    }
}
