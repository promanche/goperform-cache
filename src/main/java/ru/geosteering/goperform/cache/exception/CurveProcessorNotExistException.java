package ru.geosteering.goperform.cache.exception;

public class CurveProcessorNotExistException extends RuntimeException {

    public CurveProcessorNotExistException() {
        super("In loading queue. Try later");
    }
}
