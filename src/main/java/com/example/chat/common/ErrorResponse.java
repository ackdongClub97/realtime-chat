package com.example.chat.common;

public record ErrorResponse(String code, String message, String traceId) {
}
