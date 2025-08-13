# Конфигурация сборки

В корне проекта или (рекомендуется) в ``~/.gradle`` создать файл  ``gradle.properties``:
```properties
nexusUrl=https://nexus.geosteering.ru/
nexusUsername=username
nexusPassword=password

# Данная настройка необходима только при сборке и публикации докер-образа
# dockerTargetRegistryUrl=lib.geosteering.ru:5001
dockerTargetRegistryUrl=nexus.geosteering.ru:5001
```


# Подготовка БД

Пока что кэш не умеет самостоятельно создавать таблицы (todo: уточнить, liquibase внедрён, может уже и уметь),  поэтому создание таблиц требуется проводить до первого запуска:

```sql
create user goperform_user password 'password';
create database goperform_db owner goperform_user;
```

**Важно!** Создавать таблицы следует от имени пользователя, под которым будет работать приложение:

```shell
cat CreateTables.sql | psql postgresql://goperform_user:password@db_host:5432/goperform_db
```


# Сборка и запуск bootJar

1. Клонировать ветку dev, перейти в папку с проектом.
2. ``./gradlew bootJar``
3. Собранный jar искать в папке ``../build/libs``
5. Настроить параметры в application.properties
6. Запускать командой: \
``java -jar ***путь_и_имя_jar_файла*** --spring.config.location=***путь_и_имя_application.properties*** > /dev/null 2>&1 &`` \
\
пример:
```shell
java \
 -jar /opt/goperform-cache/cache.jar \
 --spring.config.location=/opt/goperform-cache/application.properties \
 > /dev/null 2>&1 &
```


# Сборка и запуск в Docker

## Предусловия

1. На сборочном хосте должен быть запущен docker daemon (при большом желании можно настроиться и на удалённый сервер, RTFM).
2. Указать урл, логин и пароль репо для публикации в файле ``~/.docker/config.json``:
```json
{
    "auths": {
        "lib.geosteering.ru:5001": {
            "username": "user",
            "password": "password"
        }
    }
}
```

## Собрать образ

```shell
./gradlew dockerBuildImage
```

## Опубликовать образ

```shell
./gradlew dockerPushImage
```

## Запустить приложение

Приложение запускается командой:

```shell
docker run --rm --name goperform-cache-service \
  -p 9010:9010 \
  -v $(pwd)/app.creds:/app/app.creds \
  -e GOSTREAM_DATA_SERVICE_URL="http://localhost:9002/api"
  -e DB_URL="jdbc:postgresql://localhost:5432/goperform_cache"
  -e DB_USR="goperform_cache"
  -e DB_PSW="goperform_cache"
  -e NATS_URL="nats://localhost:4222"
  -e GOPERFORM_STATUS_TOPIC= "curves.status.csv" топик для отправки загрузки статусов кривых
  nexus.geosteering.ru:5001/gostream/goperform-cache-service:latest
```

В переменных окружения (`-e ...`) должны быть указаны корректные значения

В директории, откуда будет выполняться эта команда, должен находиться файл `app.creds`


# Экспорт логов в Graylog

Чтобы логи отправлялись в Graylog необходимо добавить следующие переменные окружения:

* LOG_CONFIG=classpath:logback-env.xml
* GRAYLOG_SOURCE - указать условное имя хоста, например, `perform.corp.geosteering.ru`

Имя хоста не обязательно должно совпадать с реальным именем хоста, где развернуто приложение - это скорее способ отличать стенды

