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
import ru.geosteering.goperform.cache.auth.AuthManager;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.rest.*;
import ru.geosteering.goperform.cache.service.CurveService;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/v1/curve")
@RequiredArgsConstructor
@Slf4j
public class CurveController {

    private final CurveService service;
    private final Config config;
    private final AuthManager authManager;

    @GetMapping("/writable")
    public ResponseEntity<Boolean> isWritable(Authentication auth, @RequestParam Long id) {

        boolean result = authManager.checkObjectWriteAccess(auth, id);

        return new ResponseEntity<Boolean>(Boolean.valueOf(result), HttpStatus.OK);
    }

    @GetMapping("/{id}/coordinates/by-time")
    public ResponseEntity<List<?>> getByTime(@PathVariable Long id,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
                                             @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
                                             @RequestParam(required = false) Integer scale) {

        log.info("By-time request id {}, from {}, to {}, scale {}", id, from, to, scale);
        Double doubleFrom = from == null ? null : (double) from.toInstant().toEpochMilli();
        Double doubleTo = to == null ? null : (double) to.toInstant().toEpochMilli();
        return new ResponseEntity<>(service.getCurveData(id, doubleFrom, doubleTo, scale), HttpStatus.OK);
    }

    @GetMapping("/{id}/coordinates/by-depth")
    public ResponseEntity<List<?>> getByDepth(@PathVariable Long id) {

        log.info("By-depth request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/{id}/coordinates/image")
    public ResponseEntity<List<?>> getImage(@PathVariable Long id) {

        log.info("Image request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/{id}/coordinates/comments")
    public ResponseEntity<List<?>> getComments(@PathVariable Long id) {

        log.info("Comments request id {}", id);
        return getWithoutParams(id);
    }

    @GetMapping("/{id}")
    public ResponseEntity<CurveInfoResponse> getCurveInfo(@PathVariable Long id) {

        log.info("Curve-info request id {}", id);
        return new ResponseEntity<>(service.getCurveInfoResponse(id), HttpStatus.OK);
    }

    @GetMapping("/multi")
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

    @GetMapping("/multi/coordinates/by-time")
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

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public void reloadCurve(@PathVariable Long id) {
        log.info("Reload curve {} request", id);
        service.reloadCurve(id);
    }

    @DeleteMapping("/manually/{id}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteCurve(@PathVariable Long id){
        log.info("Curve {} removed", id);
        service.deleteCurve(id);
    }

    @PostMapping()
    @PreAuthorize("@authManager.checkObjectWriteAccess(authentication, #request.logId)")
    public ResponseEntity<Long> create(@RequestBody @Validated CreateCurveRequest request,
                                       Authentication authentication) {

        log.info("Create curve request {}", request);
        return new ResponseEntity<>(service.createCurve(request, authentication.getName()), HttpStatus.OK);
    }

    @PostMapping("/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void addComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        log.info("Add comment request id {}, comment {}", id, comment);
        service.writeComment(id, comment, authentication.getName(), false);
    }

    @PutMapping("/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void updateComment(@PathVariable Long id, @RequestBody @Validated Comment comment, Authentication authentication) {

        log.info("Update comment request id {}, comment {}", id, comment);
        service.writeComment(id, comment, authentication.getName(), true);
    }

    @DeleteMapping("/{id}/comments")
    @ResponseStatus(HttpStatus.OK)
    public void deleteComment(@PathVariable Long id, @RequestParam Double key, Authentication authentication) {

        log.info("Delete comment request id {}, key {}", id, key);
        service.removeComment(id, key, authentication.getName());
    }

    private ResponseEntity<List<?>> getWithoutParams(Long id) {
        return new ResponseEntity<>(service.getCurveData(id, null, null, null), HttpStatus.OK);
    }
}
