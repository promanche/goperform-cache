package ru.geosteering.goperform.cache.processor.request;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * Потокобезопасная очередь задач для обработки запросов.
 * <p>
 * Очередь организована как ({@link PriorityQueue}), где задачи сортируются по приоритету.
 * Все операции с очередью синхронизированы для обеспечения потокобезопасности.
 * <p>
 * Каждая задача ({@link RequestTask}) имеет уникальный идентификатор кривой (curveId) и приоритет.
 * Если задача с таким же curveId уже существует в очереди, указываем тип с высшим приоритетом между текущим типом и новым типом.
 */
class SynchronizedRequestQueue {

    /**
     * Очередь задач, отсортированная по приоритету.
     */
    private final PriorityQueue<RequestTask> queue;

    /**
     * Конструктор. Инициализирует очередь с компаратором, который сортирует задачи по их приоритету.
     */
    public SynchronizedRequestQueue() {
        this.queue = new PriorityQueue<>(Comparator.comparing(RequestTask::getPriority));
    }

    /**
     * Добавляет новую задачу в очередь.
     * <p>
     * Если задача с таким же curveId уже существует в очереди:
     * - Если приоритет новой задачи выше, заменяет существующую задачу на новую.
     * - Если приоритет новой задачи ниже или равен, задача не добавляется.
     * <p>
     * Если задачи с таким curveId нет в очереди, она добавляется.
     *
     * @param newTask Новая задача для добавления в очередь. Не должен быть null.
     */
    public synchronized void add(RequestTask newTask) {
        Iterator<RequestTask> iterator = queue.iterator();
        while (iterator.hasNext()) {
            RequestTask task = iterator.next();
            if (Objects.equals(task.getCurveId(), newTask.getCurveId())) {
                // Если приоритет существующей задачи больше или равен новой, ничего не делаем
                if (task.getPriority() >= newTask.getPriority()) {
                    return;
                }
                // Если приоритет новой задачи выше, удаляем старую задачу
                iterator.remove();
                break;
            }
        }

        queue.add(newTask);
    }

    /**
     * Очищает очередь, удаляя все задачи.
     */
    public synchronized void clear() {
        queue.clear();
    }

    /**
     * Удаляет задачи из очереди, соответствующие заданному условию.
     *
     * @param filter Условие для удаления задач.
     */
    public synchronized void removeIf(Predicate<? super RequestTask> filter) {
        queue.removeIf(filter);
    }

    /**
     * Извлекает и удаляет задачу с наивысшим приоритетом из очереди.
     *
     * @return Задача с наивысшим приоритетом или null, если очередь пуста.
     */
    public synchronized RequestTask poll() {
        return queue.poll();
    }

    /**
     * Проверяет, пуста ли очередь.
     *
     * @return true, если очередь пуста; иначе false.
     */
    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }
}