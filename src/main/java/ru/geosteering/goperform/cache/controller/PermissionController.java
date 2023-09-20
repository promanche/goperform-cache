package ru.geosteering.goperform.cache.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import ru.geosteering.goperform.cache.service.PermissionService;

@RestController
@RequestMapping("/v1/permission")
@RequiredArgsConstructor
@Slf4j
public class PermissionController {

    private final PermissionService service;

    @GetMapping("/homefolder")
    public String getHomefolderPermission(Authentication authentication) {
        log.info("{} homefolder permission request", authentication.getName());
        return service.getHomefolderPermission(authentication);
    }
}
