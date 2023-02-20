package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.TLUserObjectIn;
import ru.geosteering.commonModels.webService.responses.ApiResult;
import ru.geosteering.goperform.cache.auth.AuthManager;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class PermissionService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final Config config;
    private final AuthManager authManager;

    @SuppressWarnings("all")
    public synchronized String getHomefolderPermission(Authentication authentication) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth((String) authentication.getCredentials());
        ResponseEntity<String> response = restTemplate.exchange(config.DATA_SERVICE_BASEURL + "/objects", HttpMethod.GET, new HttpEntity<>(headers), String.class);
        log.debug(response.toString());
        try {
            if (response.getStatusCode() == HttpStatus.OK && response.hasBody()) {
                ApiResult apiResult = StaticMapper.parseObject(response.getBody(), ApiResult.class);
                if (apiResult.getStatus() == EResult.OK) {
                    List<Map<String, Object>> objects = (List<Map<String, Object>>) apiResult.getResult();
                    Map<String, Object> homefolder = objects.stream()
                            .filter(map -> map.get("parent").equals("#") && map.get("type").equals("DB"))
                            .findFirst()
                            .orElse(null);

                    if (homefolder != null && homefolder.containsKey("id")) {
                        String id = (String) homefolder.get("id");
                        if (authManager.checkObjectWriteAccess(authentication, Long.parseLong(id))) {
                            if (authManager.checkObjectAccess(authentication, Long.parseLong(id), TLUserObjectIn.Permissions.CONTROL)) {
                                return "CONTROL";
                            }
                            return "WRITE";
                        }
                        if (authManager.checkObjectReadAccess(authentication, Long.parseLong(id))) {
                            return "READ";
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return "NONE";
    }
}
