package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.service.RestService;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class CacheController {

    private final RestService service;

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<List<CacheItem>> getByTime(@PathVariable Long id,
                                                     @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                                     @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                                     @RequestParam(required = false) Integer limit) {

        log.info("Incoming by-time response id {}, from {}, to {}, limit {}", id, from, to, limit);

        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? null : (double) to.toInstant().toEpochMilli();

        List<CacheItem> response = service.getDataItems(id, doubleFrom, doubleTo, limit);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<List<CacheItem>> getByDepth(@PathVariable Long id) {

        log.info("Incoming by-depth response id {}", id);

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<List<CacheItem>> getImage(@PathVariable Long id) {

        log.info("Incoming image response id {}", id);

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<List<CacheItem>> getComments(@PathVariable Long id) {

        log.info("Incoming comments response id {}", id);

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}")
    public ResponseEntity<ApiMessage> getCurveInfo(@PathVariable Long id) {

        log.info("Incoming curve-info response id {}", id);

        ApiMessage curveInfo = service.getCurveInfo(id);

        if (curveInfo != null) {
            return new ResponseEntity<>(curveInfo, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    }

    private ResponseEntity<List<CacheItem>> getWithoutParams(Long id) {
        List<CacheItem> response = service.getDataItems(id, null, null, null);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
