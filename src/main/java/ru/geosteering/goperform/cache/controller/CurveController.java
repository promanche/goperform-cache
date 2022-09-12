package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.rest.*;
import ru.geosteering.goperform.cache.service.CurveService;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class CurveController {

    private final CurveService service;
    private final Config config;

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<List<?>> getByTime(@PathVariable Long id,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                             @RequestParam(required = false) Integer scale) {

        log.info("By-time request id {}, from {}, to {}, scale {}", id, from, to, scale);

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        if (scale != null && !config.SCALE_MINUTES.contains(scale)) {
            log.info("Scale {} not provided by configuration {}", scale, config.SCALE_MINUTES);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? null : (double) to.toInstant().toEpochMilli();

        List<?> response = service.getCurveData(id, doubleFrom, doubleTo, scale);

        return response == null ? new ResponseEntity<>(HttpStatus.ACCEPTED) : new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<List<?>> getByDepth(@PathVariable Long id) {

        log.info("By-depth request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<List<?>> getImage(@PathVariable Long id) {

        log.info("Image request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<List<?>> getComments(@PathVariable Long id) {

        log.info("Comments request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/curve/{id}")
    public ResponseEntity<CurveInfoResponse> getCurveInfo(@PathVariable Long id) {

        log.info("Curve-info request id {}", id);

        CurveInfoResponse response = service.getCurveInfoResponse(id);

        return response == null ? new ResponseEntity<>(HttpStatus.ACCEPTED) : new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<Void> reloadByTime(@PathVariable Long id,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from) {

        log.info("Reload by-time request id {}, from {}", id, from);

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();

        return service.reloadCurve(id, doubleFrom) ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @DeleteMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<Void> reloadByDepth(@PathVariable Long id,
                                              @RequestParam(required = false) Double from) {

        log.info("Reload by-depth request id {}, from {}", id, from);

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        Double doubleFrom = from == null ? Double.MIN_VALUE : from;

        return service.reloadCurve(id, doubleFrom) ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @PostMapping("/curve")
    @PreAuthorize("@authManager.checkObjectWriteAccess(#authentication, #request.logId)")
    public ResponseEntity<Long> create(@RequestBody @Validated CreateCurveRequest request,
                                       Authentication authentication) {

        Long id = service.createCurve(request, authentication.getName());

        if (id != null) {
            return new ResponseEntity<>(id, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @PostMapping("/curve/{id}/comments")
    public ResponseEntity<Void> addComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        if (service.writeComment(id, comment, authentication.getName(), false)) {
            return new ResponseEntity<>(HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @PutMapping("/curve/{id}/comments")
    public ResponseEntity<Void> updateComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        if (service.writeComment(id, comment, authentication.getName(), true)) {
            return new ResponseEntity<>(HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @DeleteMapping("/curve/{id}/comments")
    public ResponseEntity<Void> deleteComment(@PathVariable Long id, @RequestParam Double key, Authentication authentication) {

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        if (service.removeComment(id, key, authentication.getName())) {
            return new ResponseEntity<>(HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity<List<?>> getWithoutParams(Long id) {

        if (service.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        List<?> response = service.getCurveData(id, null, null, null);

        return response == null ? new ResponseEntity<>(HttpStatus.ACCEPTED) : new ResponseEntity<>(response, HttpStatus.OK);
    }
}
