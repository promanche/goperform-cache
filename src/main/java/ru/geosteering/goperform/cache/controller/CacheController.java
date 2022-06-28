package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.service.HistoryLoader;

@RestController
@RequestMapping("api/v1/history")
@RequiredArgsConstructor
public class CacheController {

    private final HistoryLoader loader;

    @GetMapping
    public ResponseEntity<CacheResponse> get(@RequestParam Long id,
                                             @RequestParam(required = false) Double from,
                                             @RequestParam(required = false) Double to) {

        CacheResponse cacheResponse = loader.getCacheResponse(id, from, to);

        if (cacheResponse == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(cacheResponse, HttpStatus.OK);
    }
}
