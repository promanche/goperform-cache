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
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.model.rest.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.CurveDispatcher;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurveService {

    private final CurveDispatcher curveDispatcher;

    public boolean isBroken(Long id) {
        return curveDispatcher.isBroken(id);
    }

    public List<?> getCurveData(Long id, Double from, Double to, Integer scale) {

        log.debug("Begin response preparing for id {}", id);

        if (from != null || to != null) {
            from = from == null ? Double.MIN_VALUE : from;
            to = to == null ? Double.MAX_VALUE : to;
        }

        List<?> result = curveDispatcher.getCurveData(id, from, to, scale);

        if (result != null) {
            log.info("Response for id {} prepared. Result list size: {}", id, result.size());
        }

        return result;
    }

    public boolean reloadCurve(Long id, Double from) {
        return curveDispatcher.reload(id, from);
    }

    public CurveInfoResponse getCurveInfoResponse(Long id) {
        return curveDispatcher.getCurveInfoResponse(id);
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

        ExtraCurveInfo curveInfo = curveDispatcher.getCurveInfo(id);

        if (curveInfo == null) {
            return false;
        }

        String from = curveInfo.getIndexType() == LogIndexType.MEASURED_DEPTH ?
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() :
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);

        CurveDataClearRequest request = new CurveDataClearRequest();
        request.setCurveId(id);
        request.setFrom(from);
        request.setTo(from);
        request.setUser(user);
        request.setUpdateBaseTimestamp(OffsetDateTime.now(ZoneId.of("Z")));

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
