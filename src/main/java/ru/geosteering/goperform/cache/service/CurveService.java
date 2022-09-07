package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.*;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.StatusMessage;
import ru.geosteering.commonModels.wits.RecordIndex;
import ru.geosteering.goperform.cache.memcache.*;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.CurveSegment;
import ru.geosteering.goperform.cache.model.rest.Comment;
import ru.geosteering.goperform.cache.model.rest.CreateCurveRequest;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.CurveDataLoadProcessor;
import ru.geosteering.goperform.cache.processor.CurveSegmentProcessor;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurveService {

    private final MainRepository repository;
    private final MetaDataCache metaDataCache;
    private final HistoryDataCache historyDataCache;
    private final RealtimeDataCache realtimeDataCache;
    private final CurveSegmentProcessor segmentProcessor;
    private final CurveDataLoadProcessor dataLoadProcessor;

    public Object getCurveData(Long id, Double from, Double to, Integer scale) {

        if (metaDataCache.isHistoryLoaded(id)) {

            log.debug("Begin response preparing for id {}", id);

            if (from != null || to != null) {
                from = from == null ? Double.MIN_VALUE : from;
                to = to == null ? Double.MAX_VALUE : to;
            }

            if (scale != null && metaDataCache.getMetaData(id).getScaleSet().contains(scale)) {

                List<CurveSegment> result = repository.getSegmentsFromTo(id, scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                        .collect(Collectors.toList());

                List<CurveItem> fromCache = historyDataCache.get(id, from, to);
                fromCache.addAll(realtimeDataCache.get(id, from, to));

                result.addAll(segmentProcessor.getAndCompleteSegmentsFromMemory(id, scale, from, to, fromCache));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;

            } else {

                List<CurveItem> result = repository.getItemsFromTo(id, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                        .collect(Collectors.toList());

                result.addAll(historyDataCache.get(id, from, to));
                result.addAll(realtimeDataCache.get(id, from, to));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;
            }
        }

        dataLoadProcessor.loadByRequest(id);

        log.debug("Curve data id {} not yet loaded", id);

        return null;
    }

    public void reloadCurve(Long id, Double from) {
        dataLoadProcessor.reloadByRequest(id, from);
    }

    public Long createCurve(CreateCurveRequest req, String user) {

        CurveInfo info = new CurveInfo();
        info.setMnemonic(req.getCurveName());
        info.setClassWitsml(req.getTypeCurve().name());

        CurveAddRequest request = new CurveAddRequest();
        request.setParentId(req.getLogId());
        request.setCurveInfo(info);
        request.setUser(user);

        Message message = NatsConnector.sendRequest("gostream.curvesAdd", StaticMapper.toBytes(request));

        ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

        if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

            StatusMessage statusMessage = (StatusMessage) apiMessage;

            if (statusMessage.getStatus() == EResult.OK) {
                return Long.valueOf(statusMessage.getMessage());
            }
        }

        return null;
    }

    public boolean writeComment(Long id, Comment comment, String user, boolean update) {

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
        request.setUser(user);
        request.setUpdate(update);

        Message message = NatsConnector.sendRequest("gostream.curvesStore", StaticMapper.toBytes(request));

        ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

        boolean written = false;

        if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

            StatusMessage statusMessage = (StatusMessage) apiMessage;

            written = statusMessage.getStatus() == EResult.OK && Integer.parseInt(statusMessage.getMessage()) > 0;

            if (update & written) {
                reloadCurve(id, comment.getKey());
            }
        }

        return written;
    }

    public boolean removeComment(Long id, Double key, String user) {

        String from = metaDataCache.getMetaData(id).getIndexType() == LogIndexType.MEASURED_DEPTH ?
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() :
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);

        CurveDataClearRequest request = new CurveDataClearRequest();
        request.setCurveId(id);
        request.setFrom(from);
        request.setTo(from);
        request.setUser(user);

        Message message = NatsConnector.sendRequest("gostream.curvesClear", StaticMapper.toBytes(request));

        ApiMessage apiMessage = StaticMapper.parseObject(new String(message.getData()), ApiMessage.class);

        boolean removed = false;

        if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.STATUS) {

            StatusMessage statusMessage = (StatusMessage) apiMessage;

            removed = statusMessage.getStatus() == EResult.OK && Integer.parseInt(statusMessage.getMessage()) > 0;

            if (removed) {
                reloadCurve(id, key);
            }
        }

        return removed;
    }
}
