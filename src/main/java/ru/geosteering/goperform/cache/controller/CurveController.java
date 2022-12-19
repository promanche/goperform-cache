package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import ru.geosteering.goperform.cache.auth.AuthManager;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.auth.CheckObjectAccessRequest;
import ru.geosteering.goperform.cache.model.rest.Comment;
import ru.geosteering.goperform.cache.model.rest.CreateCurveRequest;
import ru.geosteering.goperform.cache.model.rest.CurveInfoResponse;
import ru.geosteering.goperform.cache.model.rest.MultiResponse;
import ru.geosteering.goperform.cache.service.CurveService;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class CurveController {

    private final CurveService service;
    private final Config config;
    private final AuthManager authManager;

    @GetMapping("/curve/writable")
    public ResponseEntity<Boolean> isWritable(Authentication auth, @RequestParam Long id) {
        
        boolean result = authManager.checkObjectAccess(auth, id, CheckObjectAccessRequest.Permissions.WRITE);

        return new ResponseEntity<Boolean>(Boolean.valueOf(result), HttpStatus.OK);
    }

    @GetMapping("/curve/{id}/coordinates/by-time")
    public ResponseEntity<List<?>> getByTime(@PathVariable Long id,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                             @RequestParam(required = false) Integer scale) {

        log.info("By-time request id {}, from {}, to {}, scale {}", id, from, to, scale);
        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? null : (double) to.toInstant().toEpochMilli();
        return new ResponseEntity<>(service.getCurveData(id, doubleFrom, doubleTo, scale), HttpStatus.OK);
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
        return new ResponseEntity<>(service.getCurveInfoResponse(id), HttpStatus.OK);
    }

    @GetMapping("/curve/multi")
    @PreAuthorize("@authManager.checkBatchReadAccess(authentication, #ids)")
    public ResponseEntity<CurveInfoResponse[]> getCurveInfo(@RequestParam Long[] ids) {
        log.info("Curve-info request ids {}", Arrays.toString(ids));
        CurveInfoResponse[] infos = service.getCurveInfoResponse(ids);
        for (CurveInfoResponse cir : infos) {
            if (cir == null) {
                return new ResponseEntity<>(infos, HttpStatus.ACCEPTED);
            }
        }
        return new ResponseEntity<>(infos, HttpStatus.OK);
    }

    @GetMapping("/curve/multi/coordinates/by-time")
    @PreAuthorize("@authManager.checkBatchReadAccess(authentication, #ids)")
    @ResponseStatus(HttpStatus.OK)
    public MultiResponse getByTime(@RequestParam long[] ids,
                                   @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                   @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                   @RequestParam(required = false) Integer scale) {

        log.info("By-time request ids {}, from {}, to {}, scale {}", ids, from, to, scale);
        Double doubleFrom = from == null ? config.MIN_TIME_MILLIS : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? OffsetDateTime.now().toInstant().toEpochMilli() : (double) to.toInstant().toEpochMilli();

        return service.getMultiResponse(ids, doubleFrom, doubleTo, scale);
    }

    @DeleteMapping("/curve/{id}/coordinates/by-time")
    @ResponseStatus(HttpStatus.OK)
    public void reloadByTime(@PathVariable Long id,
                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from) {

        log.info("Reload by-time request id {}, from {}", id, from);
        Double doubleFrom = from == null ? 0 : (double) from.toInstant().toEpochMilli();
        service.reloadCurve(id, doubleFrom);
    }

    @DeleteMapping("/curve/{id}/coordinates/by-depth")
    @ResponseStatus(HttpStatus.OK)
    public void reloadByDepth(@PathVariable Long id,
                              @RequestParam(required = false) Double from) {

        log.info("Reload by-depth request id {}, from {}", id, from);
        Double doubleFrom = from == null ? Double.MIN_VALUE : from;
        service.reloadCurve(id, doubleFrom);
    }

    @PostMapping("/curve")
    @PreAuthorize("@authManager.checkObjectWriteAccess(authentication, #request.logId)")
    public ResponseEntity<Long> create(@RequestBody @Validated CreateCurveRequest request,
                                       Authentication authentication) {

        log.info("Create curve request {}", request);
        return new ResponseEntity<>(service.createCurve(request, authentication.getName()), HttpStatus.OK);
    }

    @PostMapping("/curve/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void addComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        log.info("Add comment request id {}, comment {}", id, comment);
        service.writeComment(id, comment, authentication.getName(), false);
    }

    @PutMapping("/curve/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void updateComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        log.info("Update comment request id {}, comment {}", id, comment);
        service.writeComment(id, comment, authentication.getName(), true);
    }

    @DeleteMapping("/curve/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void deleteComment(@PathVariable Long id, @RequestParam Double key, Authentication authentication) {

        log.info("Delete comment request id {}, key {}", id, key);
        service.removeComment(id, key, authentication.getName());
    }

    private ResponseEntity<List<?>> getWithoutParams(Long id) {
        return new ResponseEntity<>(service.getCurveData(id, null, null, null), HttpStatus.OK);
    }
}
