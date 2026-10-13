package com.example.chat.config;

import java.util.List;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI chatOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Realtime Chat + Event-sourced Restore API")
                        .version("0.1.0")
                        .description("""
                                1:1 실시간 채팅 세션 API.
                                - 모든 상태 변화는 append-only 이벤트 원장에 seq(세션별 gap-free)와 함께 저장된다.
                                - 중복: clientEventId 멱등성 키 (재전송 시 같은 값 → DUPLICATE + 기존 seq)
                                - 순서: 서버 발급 seq 가 유일한 정렬 기준
                                - 실시간 송수신은 WebSocket `/ws` (docs/websocket-protocol.md)
                                - 사용자 식별은 `X-User-Id` 헤더 (인증은 과제 비목표)
                                """))
                .servers(List.of(new Server().url("http://localhost:8080").description("nginx (app1, app2)")));
    }
}
