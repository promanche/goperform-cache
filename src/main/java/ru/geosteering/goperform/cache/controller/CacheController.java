package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
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
@RequestMapping()
@RequiredArgsConstructor
public class CacheController {

    private final RestService service;

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<List<CacheItem>> getByTime(@PathVariable Long id,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                                     @RequestParam(required = false) Integer limit) {

        double doubleFrom = from.toInstant().toEpochMilli();
        double doubleTo = to.toInstant().toEpochMilli();

        List<CacheItem> response = service.getDataItems(id, doubleFrom, doubleTo, true, limit);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<List<CacheItem>> getByDepth(@PathVariable Long id) {

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<List<CacheItem>> getImage(@PathVariable Long id) {

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<List<CacheItem>> getComments(@PathVariable Long id) {

        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}")
    public ResponseEntity<ApiMessage> getCurveInfo(@PathVariable Long id) {

        ApiMessage curveInfo = service.getCurveInfo(id);

        if (curveInfo != null) {
            return new ResponseEntity<>(curveInfo, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    }

    private ResponseEntity<List<CacheItem>> getWithoutParams(Long id) {
        List<CacheItem> response = service.getDataItems(id, null, null, false, null);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
