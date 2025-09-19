package ru.geosteering.goperform.cache.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.nats.client.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.CurveAddRequest;
import ru.geosteering.commonModels.dataService.requests.CurveDataClearRequest;
import ru.geosteering.commonModels.dataService.requests.CurveDataStoreRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.StatusMessage;
import ru.geosteering.commonModels.wits.RecordIndex;
import ru.geosteering.goperform.cache.auth.AuthManager;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.BadRequestException;
import ru.geosteering.goperform.cache.exception.BrokenCurveException;
import ru.geosteering.goperform.cache.exception.CurveProcessorNotExistException;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.CurveSegment;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.model.rest.Comment;
import ru.geosteering.goperform.cache.model.rest.CreateCurveRequest;
import ru.geosteering.goperform.cache.model.rest.CurveInfoResponse;
import ru.geosteering.goperform.cache.model.rest.MultiResponse;
import ru.geosteering.goperform.cache.model.ws.ProcessorMessage;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.CurveDispatcher;
import ru.geosteering.goperform.cache.processor.SingleCurveProcessor;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurveService {

    private final CurveDispatcher curveDispatcher;
    private final Config config;
    private final MainRepository repository;
    private final AuthManager authManager;
    private final CurveSimplificationService curveSimplificationService;

    private void checkCurve(Long id, Integer scale) {
        if (curveDispatcher.getCurveProcessor(id, true) == null) {
            throw new CurveProcessorNotExistException();
        }

        if (curveDispatcher.isBroken(id)) {
            throw new BrokenCurveException(id);
        }

        if (scale != null && !config.SCALE_MINUTES.contains(scale)) {
            throw new BadRequestException("Scale " + scale + " not provided by configuration");
        }
    }

    public List<?> getCurveData(Long id, Double from, Double to, Integer scale) {

        checkCurve(id, scale);

        log.debug("Begin response preparing for id {}", id);

        if (from != null || to != null) {
            from = from == null ? Double.MIN_VALUE : from;
            to = to == null ? Double.MAX_VALUE : to;
        }
        List<?> result;
        if (isApproximatedScale(scale)) {
            result = getSegments(id, from, to, scale);
        } else {
            result = getItems(id, from, to);
        }

        log.info("Response for id {} prepared. Result list size: {}", id, result.size());

        return result;
    }

    private List<?> getItems(Long id, Double from, Double to) {
        var result = repository.getItemsFromTo(id, from, to)
                .stream()
                .flatMap(str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                .collect(Collectors.toList());

        var curveProcessor = curveDispatcher.getCurveProcessor(id, true);
        result.addAll(curveProcessor.getTail(from, to));

        return result;
    }

    /**
     * Метод возвращает данные кривой(точки, штрихи) в линейном формате.
     * Линейный формат представляет собой список непрерывающихся элементов с одинаковым шагом,
     * 0й - элемент начало чанка, 1й - шаг и далее последовательность значений элементов
     *
     * @param id
     * @param from
     * @param to
     * @param scale
     * @return массив чанков
     */
    public List<List<Object>> getLinearCurveData(Long id, Double from, Double to, Integer scale) {
        checkCurve(id, scale);

        log.debug("Begin response preparing for id {}", id);

        if (from != null || to != null) {
            from = from == null ? Double.MIN_VALUE : from;
            to = to == null ? Double.MAX_VALUE : to;
        }
        List<List<Object>> result;
        if (isApproximatedScale(scale)) {
            result = getLinearSegments(id, from, to, scale);
        } else {
            result = getLinearItems(id, from, to);
        }

        log.info("Response for id {} prepared. Result list size: {}", id, result.size());

        return result;
    }

    public List<CurveItem> getSimplifiedCurveData(Long id, Double from, Double to, double epsilon) {
        checkCurve(id, null);

        log.debug("Begin response preparing for id {}", id);

        var items = repository.getItemsFromTo(id, from, to)
                .stream()
                .flatMap(str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                .toList();
        List<CurveItem> result = curveSimplificationService.simplify(items, epsilon);
        log.info("Simplified CurveData response for id {} prepared. Result list size: {}", id, result.size());

        return result;
    }

    private List<List<Object>> getLinearSegments(Long id, Double from, Double to, Integer scale) {
        var result = repository.getSegmentsFromTo(id, scale, from, to)
                .stream()
                .map(str -> StaticMapper.parseListOf(str, CurveSegment.class))
                .flatMap(segments -> mapToLinearSegments(segments).stream())
                .collect(Collectors.toList());

        var curveProcessor = curveDispatcher.getCurveProcessor(id, true);
        var segmentProcessor = curveProcessor.getSegmentProcessor();
        List<CurveSegment> segmentFromCache = segmentProcessor.getSegmentFromCache(from, to, scale);
        segmentProcessor.addItemsToSegments(curveProcessor.getTail(from, to), scale, segmentFromCache);

        result.addAll(mapToLinearSegments(segmentFromCache));

        return result;
    }

    private List<List<Object>> getLinearItems(Long id, Double from, Double to) {
        var result = repository.getItemsFromTo(id, from, to)
                .stream()
                .map(str -> StaticMapper.parseListOf(str, CurveItem.class))
                .flatMap(items -> mapToLinearItems(items).stream())
                .collect(Collectors.toList());

        var curveProcessor = curveDispatcher.getCurveProcessor(id, true);
        result.addAll(mapToLinearItems(curveProcessor.getTail(from, to)));

        return result;
    }

    private List<List<Object>> mapToLinearSegments(List<CurveSegment> segments) {
        if (segments == null || segments.isEmpty()) return Collections.emptyList();
        var result = new ArrayList<List<Object>>();
        var previousItem = segments.get(0);
        var chunk = new ChunkSegment(previousItem);
        for (int i = 1; i < segments.size(); i++) {
            var segment = segments.get(i);
            if (chunk.allowsToAddNext(segment))
                chunk.addItem(segment);
            else {
                result.add(chunk.getValues());
                chunk = new ChunkSegment(segment);
            }
        }
        result.add(chunk.getValues());

        return result;
    }

    private List<List<Object>> mapToLinearItems(List<CurveItem> items) {
        if (items == null || items.isEmpty()) return Collections.emptyList();
        var result = new ArrayList<List<Object>>();
        var previousItem = items.get(0);
        var chunk = new Chunk(previousItem);
        for (int i = 1; i < items.size(); i++) {
            var item = items.get(i);
            if (chunk.allowsToAddNext(item))
                chunk.addItem(item);
            else {
                result.add(chunk.getValues());
                chunk = new Chunk(item);
            }
        }
        result.add(chunk.getValues());

        return result;
    }

    private boolean isApproximatedScale(Integer scale) {
        return scale != null && scale >= config.SEGMENT_SCALE_MIN;
    }

    private List<CurveSegment> getSegments(Long id, Double from, Double to, Integer scale) {
        var result = repository.getSegmentsFromTo(id, scale, from, to)
                .stream()
                .flatMap(str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                .collect(Collectors.toList());
        var curveProcessor = curveDispatcher.getCurveProcessor(id, true);
        var segmentProcessor = curveProcessor.getSegmentProcessor();
        result.addAll(segmentProcessor.getSegmentFromCache(from, to, scale));

        segmentProcessor.addItemsToSegments(curveProcessor.getTail(from, to), scale, result);

        return result;
    }

    public MultiResponse getMultiResponse(long[] ids, Double from, Double to, Integer scale) {
        Long[] checkedAccess = authManager.checkBatchDeniedAccess(Arrays.stream(ids).boxed().toArray(Long[]::new));
        if (checkedAccess.length == 0) {
            return null;
        }
        List<Long> checked = new ArrayList<>();
        for (long id : checkedAccess) {
            try {
                checkCurve(id, scale);
                checked.add(id);
            } catch (Exception e) {
                log.error(id + " " + e.getMessage(), e);
            }
        }
        ids = checked.stream().mapToLong(Long::longValue).toArray();

        MultiResponse response = new MultiResponse(ids);

        StopWatch swDispatch = new StopWatch();
        StopWatch swProcess = new StopWatch();
        StopWatch swParse = new StopWatch();
        StopWatch swAdd = new StopWatch();
        if (isApproximatedScale(scale)) {
            getSegments(ids, from, to, scale, swParse, swAdd, response, swDispatch, swProcess);
        } else {
            getItems(ids, from, to, swParse, swAdd, response, swDispatch, swProcess);
        }

        int size = response.getData() == null ? 0 : response.getData().size();
        log.info("Response for ids {} prepared. Result list size: {}", Arrays.toString(ids), size + "x" + ids.length);
        log.debug("getMultiResponse() operations: parse {} ms, add {} ms, dispatch {} ms, process {} ms"
                , swParse.getTotalTimeMillis()
                , swAdd.getTotalTimeMillis()
                , swDispatch.getTotalTimeMillis()
                , swProcess.getTotalTimeMillis()
        );

        return response;
    }

    private void getSegments(long[] ids, Double from, Double to, Integer scale, StopWatch swParse, StopWatch swAdd, MultiResponse response, StopWatch swDispatch, StopWatch swProcess) {
        repository.getSegmentsFromTo(ids, scale, from, to)
                .forEach(dto -> {
                    swParse.start();
                    List<CurveSegment> segments = StaticMapper.parseListOf(dto.getData(), CurveSegment.class);
                    swParse.stop();

                    swAdd.start();
                    response.addSegments(dto.getId(), segments);
                    swAdd.stop();
                });

        for (long id : ids) {
            swDispatch.start();
            SingleCurveProcessor processor = curveDispatcher.getCurveProcessor(id, true);
            swDispatch.stop();
            if (processor != null) {
                swProcess.start();
                var segmentProcessor = processor.getSegmentProcessor();
                List<CurveSegment> cachedSegments = segmentProcessor.getSegmentFromCache(from, to, scale);
                segmentProcessor.addItemsToSegments(processor.getTail(from, to), scale, cachedSegments);
                swProcess.stop();

                swAdd.start();
                response.addSegments(id, cachedSegments);
                swAdd.stop();
            }
        }
    }

    private void getItems(long[] ids, Double from, Double to, StopWatch swParse, StopWatch swAdd, MultiResponse response, StopWatch swDispatch, StopWatch swProcess) {
        repository.getItemsFromTo(ids, from, to)
                .forEach(dto -> {
                    swParse.start();
                    List<CurveItem> items = StaticMapper.parseListOf(dto.getData(), CurveItem.class);
                    swParse.stop();

                    swAdd.start();
                    response.addItems(dto.getId(), items);
                    swAdd.stop();
                });

        for (long id : ids) {
            swDispatch.start();
            SingleCurveProcessor processor = curveDispatcher.getCurveProcessor(id, true);
            swDispatch.stop();
            if (processor != null) {
                swProcess.start();
                List<CurveItem> items = processor.getTail(from, to);
                swProcess.stop();

                swAdd.start();
                response.addItems(id, items);
                swAdd.stop();
            }
        }
    }

    public void reloadCurve(Long id) {
        checkCurve(id, null);
        curveDispatcher.fullCurveReload(id);
    }

    public void reloadListCurves(List<Long> ids) {
        ids.forEach(id -> {
            checkCurve(id, null);
            curveDispatcher.fullCurveReload(id);
        });
    }

    public void deleteCurve(Long id) {
        checkCurve(id, null);
        curveDispatcher.deleteCurve(id);
    }

    public CurveInfoResponse getCurveInfoResponse(Long id) {

        Long[] checked = authManager.checkBatchDeniedAccess(new Long[]{id});
        if (checked.length == 0) {
            return null;
        }
        checkCurve(id, null);


        SingleCurveProcessor curveProcessor = curveDispatcher.getCurveProcessor(id, true);
        ExtraCurveInfo info = curveProcessor.getInfo();

        return new CurveInfoResponse(
                info.getId(),
                info.getMnemonic(),
                info.getIndexType(),
                info.getUnit(),
                info.getAxisDefinition(),
                info.getClassWitsml(),
                info.getTypeLogData(),
                info.getMaxValue(),
                info.getMinValue(),
                info.getMaxKey(),
                info.getMinKey(),
                curveProcessor.getSavedCount(),
                curveProcessor.getScaleSet(),
                info.getLastValue(),
                curveProcessor.getLoadStatus(),
                info.getMinLoadedKey(),
                info.getMaxLoadedKey(),
                curveProcessor.getReloadData() != null,
                true,
                null
        );
    }

    public List<CurveInfoResponse> getCurveInfoResponse(Long[] ids) {
        Long[] checked = authManager.checkBatchDeniedAccess(ids);
        if (checked.length == 0) {
            return null;
        }
        List<CurveInfoResponse> response = new ArrayList<>();
        List<Long> noProcessorIds = new ArrayList<>();
        Arrays.stream(checked)
                .forEach(id -> {
                    if (curveDispatcher.isCurveProcessorPresent(id)) {
                        var curveProcessor = curveDispatcher.getCurveProcessor(id, true);
                        var info = curveProcessor.getInfo();
                        var cir = map(info);
                        cir.saved(curveProcessor.getSavedCount())
                                .initializing(false)
                                .scaleSet(curveProcessor.getScaleSet())
                                .status(curveProcessor.getLoadStatus())
                                .waitReload(curveProcessor.getReloadData() != null);
                        response.add(cir.build());
                    } else {
                        noProcessorIds.add(id);
                    }
                });

        if (!noProcessorIds.isEmpty()) {
            CompletableFuture.runAsync(() -> noProcessorIds
                    .forEach(id -> {
                        var processor = curveDispatcher.getCurveProcessor(id, true);
                        if (processor != null)
                            processor.sendWsMessage(new ProcessorMessage(id));
                    }));

            repository.getInfos(noProcessorIds)
                    .forEach(info -> {
                        var cir = map(info);
                        cir.initializing(true);
                        response.add(cir.build());
                    });
        }
        return response;
    }

    private CurveInfoResponse.CurveInfoResponseBuilder map(ExtraCurveInfo info) {
        return CurveInfoResponse.builder()
                .id(info.getId())
                .mnemonic(info.getMnemonic())
                .indexType(info.getIndexType())
                .unit(info.getUnit())
                .axisDefinition(info.getAxisDefinition())
                .classWitsml(info.getClassWitsml())
                .typeLogData(info.getTypeLogData())
                .maxValue(info.getMaxValue())
                .minValue(info.getMinValue())
                .maxKey(info.getMaxKey())
                .minKey(info.getMinKey())
                .lastValue(info.getLastValue())
                .minLoadedKey(info.getMinLoadedKey())
                .maxLoadedKey(info.getMaxLoadedKey());
    }

    public Long createCurve(CreateCurveRequest req) {
        Long[] checked = authManager.checkBatchDeniedAccess(new Long[]{req.getLogId()});
        if (checked.length == 0) {
            return null;
        }
        CurveInfo info = new CurveInfo();
        info.setMnemonic(req.getCurveName());
        info.setClassWitsml(req.getTypeCurve().name());

        CurveAddRequest request = new CurveAddRequest();
        request.setParentId(req.getLogId());
        request.setCurveInfo(info);
        request.setUser(config.GOSTREAM_USERNAME);

        log.info("Request: {}", request);
        Message message = NatsConnector.sendRequest("gostream.curvesAdd", StaticMapper.toBytes(request));

        if (message != null) {
            log.info("Response: {}", message);
            ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

            if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

                StatusMessage statusMessage = (StatusMessage) apiMessage;

                if (statusMessage.getStatus() == EResult.OK) {
                    return Long.valueOf(statusMessage.getMessage());
                } else {
                    throw new BadRequestException(statusMessage.getMessage());
                }
            }
            return null;
        } else {
            log.error("Response from GOstream is null");
            throw new NullPointerException("Response from GOstream is null");
        }
    }

    public void writeComment(Long id, Comment comment, boolean update) {

        checkCurve(id, null);

        CurveDataStoreRequest.CurveStoreData data = new CurveDataStoreRequest.CurveStoreData();
        data.setCurveId(id);

        if (comment.getRecordIndex() == RecordIndex.TIME) {
            data.setDatetime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(comment.getKey().longValue()), ZoneOffset.UTC));
        } else if (comment.getRecordIndex() == RecordIndex.DEPTH) {
            data.setDepth((int) (comment.getKey() * 10000));
        }

        data.setValue(StaticMapper.toJson(comment));

        CurveDataStoreRequest request = new CurveDataStoreRequest();
        request.setIndex(comment.getRecordIndex());
        request.setData(List.of(data));
        request.setUser(config.GOSTREAM_USERNAME);
        request.setUpdate(update);

        log.info("Request: {}", request);
        Message message = NatsConnector.sendRequest("gostream.curvesStore", StaticMapper.toBytes(request));
        log.info("Response: {}", message);

        ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

        if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

            StatusMessage statusMessage = (StatusMessage) apiMessage;

            if (statusMessage.getStatus() == EResult.OK && Integer.parseInt(statusMessage.getMessage()) > 0) {
                if (update) {
                    reloadCurve(id);
                }
            } else {
                throw new BadRequestException(statusMessage.getMessage());
            }
        }
    }

    public void removeComment(Long id, Double key) {

        checkCurve(id, null);

        String from = curveDispatcher.getCurveProcessor(id, true).isDateTimeCurve() ?
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME) :
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();

        CurveDataClearRequest request = new CurveDataClearRequest();
        request.setCurveId(id);
        request.setFrom(from);
        request.setTo(from);
        request.setUser(config.GOSTREAM_USERNAME);
        request.setUpdateBaseTimestamp(OffsetDateTime.now(ZoneId.of("Z")));

        log.info("Request: {}", request);
        Message message = NatsConnector.sendRequest("gostream.curvesClear", StaticMapper.toBytes(request));
        log.info("Response: {}", message);

        ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

        if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

            StatusMessage statusMessage = (StatusMessage) apiMessage;

            if (statusMessage.getStatus() == EResult.OK && Integer.parseInt(statusMessage.getMessage()) > 0) {
                reloadCurve(id);
            } else {
                throw new BadRequestException(statusMessage.getMessage());
            }
        }
    }

    @Getter
    public static class Chunk {

        private final List<Object> values = new ArrayList<>();
        private CurveItem lastItem;

        public Chunk(CurveItem item) {
            values.add(item.getKey());
            values.add(0d);
            values.add(item.getValue());
            lastItem = item;
        }

        public boolean allowsToAddNext(CurveItem item) {
            var step = item.getKey() - lastItem.getKey();
            return this.getStep() == 0 || Double.compare(this.getStep(), step) == 0;
        }

        @JsonIgnore
        public Double getStep() {
            return (Double) values.get(1);
        }

        public void addItem(CurveItem item) {
            if (Double.compare(getStep(), 0) == 0) {
                values.set(1, item.getKey() - (Double) values.get(0));
            }
            values.add(item.getValue());
            lastItem = item;
        }
    }

    @Getter
    public static class ChunkSegment {

        private final List<Object> values = new ArrayList<>();
        private CurveSegment lastSegment;

        public ChunkSegment(CurveSegment segment) {
            lastSegment = segment;
            values.add(segment.getFirstKey());
            values.add(segment.getLastKey() - segment.getFirstKey());
            values.add(segment.getMinVal());
            values.add(segment.getMaxVal());
        }

        public boolean allowsToAddNext(CurveSegment segment) {
            var step = segment.getLastKey() - segment.getFirstKey();
            return Double.compare(this.lastSegment.getLastKey(), segment.getFirstKey()) == 0 &&
                    Double.compare(this.getStep(), step) == 0;
        }

        @JsonIgnore
        public Double getStep() {
            return (Double) values.get(1);
        }

        public void addItem(CurveSegment segment) {
            values.add(segment.getMinVal());
            values.add(segment.getMaxVal());
            lastSegment = segment;
        }
    }
}
