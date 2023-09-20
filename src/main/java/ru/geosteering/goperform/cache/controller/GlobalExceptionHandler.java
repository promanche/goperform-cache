package ru.geosteering.goperform.cache.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;
import ru.geosteering.goperform.cache.exception.*;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BadRequestException.class)
    ResponseEntity<String> handleBadRequest(BadRequestException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(CurveProcessorNotExistException.class)
    ResponseEntity<String> handleCurveProcessorNotExist(CurveProcessorNotExistException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.ACCEPTED);
    }

    @ExceptionHandler(BrokenCurveException.class)
    ResponseEntity<String> handleBrokenCurve(BrokenCurveException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.ACCEPTED);
    }

    @ExceptionHandler(RestClientException.class)
    ResponseEntity<String> handleRestClientException(RestClientException ex) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.SERVICE_UNAVAILABLE);
    }
}
