package ru.geosteering.goperform.cache.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;
import ru.geosteering.commonModels.webService.JSTreeResponse;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class ApiServiceRestClientService {

    private final ApiServiceAuthenticationService authenticationService;
    private final WebClient apiServiceClient;

    public Map<WellState, List<Long>> getAllWellsCurves(List<Long> ids) {

        String jwtToken = authenticationService.authenticate();
        if (jwtToken == null) {
            log.warn("Authentication failed");
            return Map.of();
        }
        log.debug("Authenticated");

        long started = System.currentTimeMillis();
        Map<WellState, List<Long>> wellCurves = new HashMap<>();
        ids.forEach(id -> {

            List<Long> list = wellCurves.values().stream().flatMap(List::stream).toList();
            if (!list.contains(id)) {
                List<Long> curves = new ArrayList<>();

                JSTreeResponse jsTreeResponse = getObject(jwtToken,id);
                if (jsTreeResponse != null) {
                    while (true) {

                        if (jsTreeResponse.getType().equals("WELL")
                                || jsTreeResponse.getType().equals("WELL_RED")
                                || jsTreeResponse.getType().equals("WELL_YELLOW")
                                || jsTreeResponse.getType().equals("WELL_GREEN")) {
                            WellState wellState = new WellState(Long.parseLong(jsTreeResponse.getId()), jsTreeResponse.getType());
                            List<Long> listCurves = wellCurves.getOrDefault(wellState, new ArrayList<>());
                            curves.retainAll(ids);
                            listCurves.addAll(curves);
                            wellCurves.put(wellState, listCurves);
                            break;
                        }

                        jsTreeResponse = getObject(jwtToken,Long.parseLong(jsTreeResponse.getParent()));

                        if (jsTreeResponse != null) {
                            if (jsTreeResponse.getType().equals("WELLBORE")) {
                                log.debug("Wellbore {} for curve {}", jsTreeResponse.getId(), id);
                                List<JSTreeResponse> objectsCurves =
                                        getObjects(jwtToken,Long.parseLong(jsTreeResponse.getId()), true);
                                List<Long> logCurves = objectsCurves
                                        .stream().filter(treeResponse -> treeResponse.getType().equals("CURVE"))
                                        .map(object -> Long.parseLong(object.getId())).toList();
                                curves.addAll(logCurves);
                            }
                        } else {
                            WellState wellState = new WellState(0L, "WELL");
                            List<Long> listCurves = wellCurves.getOrDefault(wellState, new ArrayList<>());
                            if (!listCurves.contains(id)) {
                                listCurves.add(id);
                                wellCurves.put(wellState, listCurves);
                                log.error("Curve {} must be removed cause it does not have log", id);
                            }
                            return;
                        }
                    }
                } else {
                    WellState wellState = new WellState(0L, "WELL");
                    List<Long> listCurves = wellCurves.getOrDefault(wellState, new ArrayList<>());
                    if (!listCurves.contains(id)) {
                        listCurves.add(id);
                        wellCurves.put(wellState, listCurves);
                        log.error("Curve {} must be removed cause it does not exist in GoStream", id);
                    }
                }
            }
        });
        log.info("{} curves received for {} wells in {} ms", wellCurves.values().stream().flatMap(List::stream).toList().size(), wellCurves.size(), System.currentTimeMillis() - started);
        return wellCurves;
    }

    /**
     * Этот метод возвращает список объектов.
     *
     * @param parentId идентификатор родительского объекта
     * @param all      если <code>true</code>, вернутся все объекты, включая кривые
     * @return List список объектов
     */

    @Nullable
    public List<JSTreeResponse> getObjects(String jwtToken,@Nullable Long parentId, boolean all) {
        log.info("List objects. all = {}", all);

        ObjectsResponse response = apiServiceClient
                .get()
                .uri(uriBuilder -> {
                    UriBuilder builder = uriBuilder
                            .path("/objects")
                            .queryParam("all", all);
                    if (parentId != null) {
                        builder.queryParam("parentId", parentId);
                    }
                    return builder.build();
                })
                .headers(httpHeaders -> httpHeaders.setBearerAuth(jwtToken))
                .retrieve()
                .bodyToMono(ObjectsResponse.class)
                .block();
        log.debug("Response allObjects {}", response);
        if (response == null || response.getResult() == null) {
            log.warn("Empty response from api-service");
            return List.of();
        }
        if (!response.getStatus().equals("OK")) {
            log.warn("Error received from api-service: {}", response.getMessage());
            return List.of();
        }
        log.info("{} items received", response.getResult().size());
        return response.getResult();
    }

    /**
     * Этот метод возвращает объект с указанным идентификатором.
     *
     * @param id идентификатор объекта
     * @return JSTreeResponse объект
     */
    @Nullable
    public JSTreeResponse getObject(String jwtToken,long id) {
        log.info("Get object. Id: {}", id);

        JSTreeResponse response = apiServiceClient
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path("/objects/{id}")
                        .build(id))
                .headers(httpHeaders -> httpHeaders.setBearerAuth(jwtToken))
                .retrieve()
                .bodyToMono(JSTreeResponse.class)
                .block();
        if (response == null) {
            log.warn("Empty response from api-service");
            return null;
        }
        log.info("Response received: {}", response);
        return response;
    }


    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class WellState {
        private Long wellId;
        private String state;
    }
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public abstract static class ApiResponse {
        private String status;
        private String message;
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ObjectsResponse extends ApiResponse {
        private List<JSTreeResponse> result;
    }
}
