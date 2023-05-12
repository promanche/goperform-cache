/**
 * Пакет для работы с NATS
 * <p>
 * {@link ru.geosteering.goperform.cache.nats.NatsConnector} Основной класс для подключения и отправки сообщений
 * <p>
 * {@link ru.geosteering.goperform.cache.nats.HistoryMessageHandler}, {@link ru.geosteering.goperform.cache.nats.RealtimeMessageHandler} Обработчки входящих сообщений
 * <p>
 * {@link ru.geosteering.goperform.cache.nats.ConnectionEventListener} Интерфейс для реакции на события подключения
 * <p>
 * {@link ru.geosteering.goperform.cache.nats.ConnectionEventDispatcher} Собирает все реализации {@link ru.geosteering.goperform.cache.nats.ConnectionEventListener}, вызывает на них соответствующие методы
 */
package ru.geosteering.goperform.cache.nats;