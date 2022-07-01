package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.service.HistoryLoader;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
@RequestMapping()
@RequiredArgsConstructor
public class CacheController {

    private final HistoryLoader loader;

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<List<CacheItem>> getByTime(@PathVariable Long id,
                                                     @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                                     @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to) {

        double doubleFrom = from.toInstant().toEpochMilli();
        double doubleTo = to.toInstant().toEpochMilli();

        List<CacheItem> response = loader.getResponse(id, doubleFrom, doubleTo, true);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<List<CacheItem>> getByDepth(@PathVariable Long id) {

        List<CacheItem> response = loader.getResponse(id, null, null, false);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<List<CacheItem>> getImage(@PathVariable Long id) {

        List<CacheItem> response = loader.getResponse(id, null, null, false);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<List<CacheItem>> getComments(@PathVariable Long id) {

        List<CacheItem> response = loader.getResponse(id, null, null, false);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
