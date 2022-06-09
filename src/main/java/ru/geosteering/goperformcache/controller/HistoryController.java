package ru.geosteering.goperformcache.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperformcache.service.HistoryService;

import java.time.OffsetDateTime;

@RestController
@RequestMapping("api/v1/history")
@RequiredArgsConstructor
public class HistoryController {

    private final HistoryService service;

    @GetMapping
    public String get(@RequestParam Long id,
                      @RequestParam(required = false) OffsetDateTime from,
                      @RequestParam(required = false) OffsetDateTime to) {
        return "Not realized";
    }
}
