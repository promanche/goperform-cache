FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY app/goperform-cache-service.jar /app/goperform-cache-service.jar

RUN apk --update --no-cache add curl

ENTRYPOINT exec java $JAVA_OPTS -jar ./goperform-cache-service.jar
EXPOSE 9010
