package ru.geosteering.goperform.cache.exception;

public class BrokenCurveException extends RuntimeException {

    public BrokenCurveException(Long id) {
        super("Can't load " + id + " curve info. Try later");
    }
}
