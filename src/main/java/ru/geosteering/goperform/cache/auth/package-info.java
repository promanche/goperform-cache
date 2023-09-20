/**
 * Содержит классы для аутентификации и авторизации пользователей.
 * <p>
 * {@link ru.geosteering.goperform.cache.auth.ObjectAccessor} проверяет доступ пользователя к объектам по id.
 * <p>
 * {@link ru.geosteering.goperform.cache.auth.AuthManager} основной класс для аутентификации и авторизации
 * пользователей. Дополнительно реализует {@link org.springframework.security.authorization.AuthorizationManager} для использования при настройке Spring Security.
 * <p>
 * {@link ru.geosteering.goperform.cache.auth.AuthFilter} реализация {@link org.springframework.web.filter.OncePerRequestFilter} для использования в SecurityFilterChain.
 */
package ru.geosteering.goperform.cache.auth;