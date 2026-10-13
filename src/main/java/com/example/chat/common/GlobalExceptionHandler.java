package com.example.chat.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException e) {
        return ResponseEntity.status(e.status()).body(body(e.code(), e.getMessage()));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, MissingRequestHeaderException.class,
            MethodArgumentTypeMismatchException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ErrorResponse> handleValidation(Exception e) {
        return ResponseEntity.badRequest().body(body("INVALID_REQUEST", e.getMessage()));
    }

    /**
     * DB 커넥션 고갈 / 타임아웃은 재시도 가능한 503 으로 응답한다. 클라이언트는 같은 clientEventId 로 재시도하면 된다.
     */
    @ExceptionHandler({CannotGetJdbcConnectionException.class, QueryTimeoutException.class,
            DataAccessResourceFailureException.class, TransientDataAccessException.class,
            TransactionException.class})
    public ResponseEntity<ErrorResponse> handleDbUnavailable(Exception e) {
        log.warn("database unavailable: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "1")
                .body(body("DB_UNAVAILABLE", "database temporarily unavailable, retry with the same clientEventId"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnknown(Exception e) {
        if (e instanceof org.springframework.web.ErrorResponse spring) {
            // 405, 415, 404(no handler) 등 Spring MVC 표준 오류는 원래 상태 코드를 유지
            return ResponseEntity.status(spring.getStatusCode()).body(body("INVALID_REQUEST", e.getMessage()));
        }
        log.error("unhandled error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body("INTERNAL_ERROR", "internal error"));
    }

    private static ErrorResponse body(String code, String message) {
        return new ErrorResponse(code, message, MDC.get("traceId"));
    }
}
