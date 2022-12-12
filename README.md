1. Клонировать ветку dev, перейти в папку с проектом.
2. Для сборки необходим файл gradle.properties в корне проекта с данными для доступа в nexus (или прописать данные в файле build.gradle). Собрать проект командой gradle bootJar.
3. Собранный jar искать в папке ../build/libs
4. Cоздать таблицы в базе данных - скрипт в файле CreateTables.sql
5. Настроить параметры в application.properties
6. Запускать командой java -jar ***путь до jar файла*** --spring.config.location=***путь до файла application.properties*** > /dev/null 2>&1 & \
(пример: *java -jar /opt/goperform-cache/cache.jar --spring.config.location=/opt/goperform-cache/application.properties > /dev/null 2>&1 &*)

## Запуск в Docker

### Как собрать образ

Образ собирается командой:

```shell
./gradlew dockerBuildImage
```

### Как запустить приложение

Приложение запускается командой:

```shell
docker run --rm --name goperform-cache-service \
  -p 9010:9010 \
  -v $(pwd)/application.properties:/app/application.properties \
  -v $(pwd)/app.creds:/app/app.creds \
  nexus.geosteering.ru:5001/gostream/goperform-cache-service:latest
```

В директории откуда будет выполняться эта команда должны находиться два файла:

* application.properties
* app.creds

### Как опубликовать образ

Образ публикуется командой:

```shell
./gradlew dockerPublishImage
```
