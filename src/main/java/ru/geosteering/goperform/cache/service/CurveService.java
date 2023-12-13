package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.*;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.StatusMessage;
import ru.geosteering.commonModels.wits.RecordIndex;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.*;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.rest.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.CurveDispatcher;
import ru.geosteering.goperform.cache.processor.SingleCurveProcessor;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurveService {

    private final CurveDispatcher curveDispatcher;
    private final Config config;
    private final MainRepository repository;

    public void checkCurve(Long id, Integer scale) {
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

        List<?> result = curveDispatcher.getCurveProcessor(id, true).getCurveData(from, to, scale);

        if (result != null) {
            log.info("Response for id {} prepared. Result list size: {}", id, result.size());
        }

        return result;
    }

    public MultiResponse getMultiResponse(long[] ids, Double from, Double to, Integer scale) {

        List<Long> checked = new ArrayList<>();
        for (long id : ids) {
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
        if (scale == null || scale < 15) {
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

        } else {
            repository.getSegmentsFromTo(ids, scale, from, to)
                    .forEach(dto -> {
                        swParse.start();
                        List<CurveSegment> items = StaticMapper.parseListOf(dto.getData(), CurveSegment.class);
                        swParse.stop();

                        swAdd.start();
                        response.addSegments(dto.getId(), items);
                        swAdd.stop();
                    });

            for (long id : ids) {
                swDispatch.start();
                SingleCurveProcessor processor = curveDispatcher.getCurveProcessor(id, true);
                swDispatch.stop();
                if (processor != null) {
                    swProcess.start();
                    List<CurveSegment> items = processor.getSegmentsFromTail(from, to, scale);
                    swProcess.stop();

                    swAdd.start();
                    response.addSegments(id, items);
                    swAdd.stop();
                }
            }
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

    public void reloadCurve(Long id) {
        checkCurve(id, null);
        curveDispatcher.fullCurveReload(id);
    }

    public CurveInfoResponse getCurveInfoResponse(Long id) {

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
                curveProcessor.getReloadData() != null);
    }

    public CurveInfoResponse[] getCurveInfoResponse(Long[] ids) {
        CurveInfoResponse[] response = new CurveInfoResponse[ids.length];

        for (int i = 0; i < response.length; i++) {
            CurveInfoResponse cir = null;
            try {
                cir = getCurveInfoResponse(ids[i]);
            } catch (Exception e) {
                log.info(ids[i] + " " + e.getMessage());
            }
            response[i] = cir;
        }

        return response;
    }

    public Long createCurve(CreateCurveRequest req, String user) {

        CurveInfo info = new CurveInfo();
        info.setMnemonic(req.getCurveName());
        info.setClassWitsml(req.getTypeCurve().name());

        CurveAddRequest request = new CurveAddRequest();
        request.setParentId(req.getLogId());
        request.setCurveInfo(info);
        request.setUser(user);

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
        }else {
            log.error("Response from GOstream is null");
            throw new NullPointerException("Response from GOstream is null");
        }
    }

    public void writeComment(Long id, Comment comment, String user, boolean update) {

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
        request.setUser(user);
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

    public void removeComment(Long id, Double key, String user) {

        checkCurve(id, null);

        String from = curveDispatcher.getCurveProcessor(id, true).isDateTimeCurve() ?
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME) :
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();

        CurveDataClearRequest request = new CurveDataClearRequest();
        request.setCurveId(id);
        request.setFrom(from);
        request.setTo(from);
        request.setUser(user);
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
}
