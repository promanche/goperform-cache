package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.service.CurveService;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.time.OffsetDateTime;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class CurveController {

    private final CurveService service;
    private final MetaDataCache metaDataCache;
    private final Config config;

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<Object> getByTime(@PathVariable Long id,
                                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                            @RequestParam(required = false) Integer scale) {

        log.info("Incoming by-time request id {}, from {}, to {}, scale {}", id, from, to, scale);

        LogIndexType indexType = metaDataCache.getMetaData(id).getIndexType();
        if (indexType == LogIndexType.MEASURED_DEPTH) {
            log.info("Invalid indextype {} for id {}", indexType, id);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        if (scale != null && !config.SCALE_MINUTES.contains(scale)) {
            log.info("Scale {} not provided by configuration {}", scale, config.SCALE_MINUTES);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? null : (double) to.toInstant().toEpochMilli();

        Object response = service.getCurveData(id, doubleFrom, doubleTo, scale);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<Object> getByDepth(@PathVariable Long id) {

        log.info("Incoming by-depth request id {}", id);

        LogIndexType indexType = metaDataCache.getMetaData(id).getIndexType();
        if (indexType != LogIndexType.MEASURED_DEPTH) {
            log.info("Invalid indextype {} for id {}", indexType, id);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<Object> getImage(@PathVariable Long id) {

        log.info("Incoming image request id {}", id);

        if (metaDataCache.getMetaData(id).getAxisDefinition() == null) {
            log.info("Curve id {} is not image", id);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<Object> getComments(@PathVariable Long id) {

        log.info("Incoming comments request id {}", id);

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}")
    public ResponseEntity<MetaData> getCurveInfo(@PathVariable Long id) {

        log.info("Incoming curve-info request id {}", id);

        MetaData metaData = service.getCurveInfo(id);

        if (metaData != null) {
            return new ResponseEntity<>(metaData, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    }

    private ResponseEntity<Object> getWithoutParams(Long id) {
        Object response = service.getCurveData(id, null, null, null);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
