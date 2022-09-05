1.  Клонировать, перейти в папку с проектом, собрать командой gradle bootJar. Собранный jar искать в папке ../build/libs
2.  Cоздать таблицы в базе данных - скрипт в файле CreateTables.sql
3.  Настроить параметры в application.properties
4.  Запускать командой java -jar ***путь до jar файла*** --spring.config.location=***путь до файла application.properties*** > /dev/null 2>&1 & \
(пример: *java -jar /opt/goperform-cache/cache.jar --spring.config.location=/opt/goperform-cache/application.properties > /dev/null 2>&1 &*)