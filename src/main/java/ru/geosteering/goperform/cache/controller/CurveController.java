package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Delete;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.model.rest.Comment;
import ru.geosteering.goperform.cache.model.rest.CreateCurveRequest;
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

        log.info("By-time request id {}, from {}, to {}, scale {}", id, from, to, scale);

        if (isDateTimeCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
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

        log.info("Curve {} invalid indexType", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @GetMapping("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<Object> getByDepth(@PathVariable Long id) {

        log.info("By-depth request id {}", id);

        if (isDepthCurve(id)) {

            return getWithoutParams(id);
        }

        log.info("Curve {} invalid indexType", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @GetMapping("/curve/{id}/coordinates/image")
    public ResponseEntity<Object> getImage(@PathVariable Long id) {

        log.info("Image request id {}", id);

        if (isImageCurve(id)) {

            return getWithoutParams(id);
        }

        log.info("Curve {} is not image", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @GetMapping("/curve/{id}/coordinates/comments")
    public ResponseEntity<Object> getComments(@PathVariable Long id) {

        log.info("Comments request id {}", id);

        if (isCommentsCurve(id)) {

            return getWithoutParams(id);
        }

        log.info("Curve {} is not comment", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @GetMapping("/curve/{id}")
    public ResponseEntity<MetaData> getCurveInfo(@PathVariable Long id) {

        log.info("Curve-info request id {}", id);

        MetaData metaData = metaDataCache.getMetaData(id);

        if (metaData != null) {
            return new ResponseEntity<>(metaData, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Delete("/curve/{id}/coordinates/by-time")
    public ResponseEntity<Void> reloadByTime(@PathVariable Long id,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from) {

        log.info("Reload by-time request id {}, from {}", id, from);

        if (isDateTimeCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
            }

            Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();

            service.reloadCurve(id, doubleFrom);

            return new ResponseEntity<>(HttpStatus.OK);
        }

        log.info("Curve {} invalid indexType", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @Delete("/curve/{id}/coordinates/by-depth")
    public ResponseEntity<Void> reloadByDepth(@PathVariable Long id,
                                              @RequestParam(required = false) Double from) {

        log.info("Reload by-depth request id {}, from {}", id, from);

        if (isDepthCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
            }

            Double doubleFrom = from == null ? Double.MIN_VALUE : from;

            service.reloadCurve(id, doubleFrom);

            return new ResponseEntity<>(HttpStatus.OK);
        }

        log.info("Curve {} invalid indexType", id);
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
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

        if (isCommentsCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
            }

            if (service.writeComment(id, comment, authentication.getName(), false)) {
                return new ResponseEntity<>(HttpStatus.OK);
            }
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @PutMapping("/curve/{id}/comments")
    public ResponseEntity<Void> updateComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        if (isCommentsCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
            }

            if (service.writeComment(id, comment, authentication.getName(), true)) {
                return new ResponseEntity<>(HttpStatus.OK);
            }
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    @DeleteMapping("/curve/{id}/comments")
    public ResponseEntity<Void> deleteComment(@PathVariable Long id, @RequestParam Double key, Authentication authentication) {

        if (isCommentsCurve(id)) {

            if (metaDataCache.isBroken(id)) {
                return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
            }

            if (service.removeComment(id, key, authentication.getName())) {
                return new ResponseEntity<>(HttpStatus.OK);
            }
        }

        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity<Object> getWithoutParams(Long id) {

        if (metaDataCache.isBroken(id)) {
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }

        Object response = service.getCurveData(id, null, null, null);

        if (response == null) {
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private boolean isDateTimeCurve(Long id) {
        return metaDataCache.getMetaData(id).getIndexType() != LogIndexType.MEASURED_DEPTH;
    }

    private boolean isDepthCurve(Long id) {
        return metaDataCache.getMetaData(id).getIndexType() == LogIndexType.MEASURED_DEPTH;
    }

    private boolean isCommentsCurve(Long id) {
        return metaDataCache.getMetaData(id).getClassWitsml().equals("COMMENTS");
    }

    private boolean isImageCurve(Long id) {
        return metaDataCache.getMetaData(id).getAxisDefinition() != null;
    }
}
