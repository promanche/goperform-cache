package ru.geosteering.goperformcache.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperformcache.controller.dto.CacheResponse;
import ru.geosteering.goperformcache.service.HistoryLoader;

import java.time.OffsetDateTime;

@RestController
@RequestMapping("api/v1/history")
@RequiredArgsConstructor
public class HistoryController {

    private final HistoryLoader loader;

    @GetMapping
    public ResponseEntity<CacheResponse> get(@RequestParam Long id,
                                             @RequestParam(required = false) OffsetDateTime from,
                                             @RequestParam(required = false) OffsetDateTime to) {

        CacheResponse cacheResponse = loader.getCacheResponse(id, from, to);

        if (cacheResponse == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(cacheResponse, HttpStatus.OK);
    }
}
