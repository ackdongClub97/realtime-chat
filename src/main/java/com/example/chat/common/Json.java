package com.example.chat.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * checked 예외를 감싼 ObjectMapper 헬퍼.
 */
@Component
public class Json {

    private final ObjectMapper mapper;

    public Json(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("json serialization failed", e);
        }
    }

    public JsonNode readTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid json", e);
        }
    }

    public <T> T read(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid json for " + type.getSimpleName(), e);
        }
    }

    public JsonNode toTree(Object value) {
        return mapper.valueToTree(value);
    }

    public ObjectNode object() {
        return mapper.createObjectNode();
    }
}
