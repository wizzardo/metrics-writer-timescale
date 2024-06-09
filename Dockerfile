FROM bellsoft/liberica-openjdk-alpine:21

WORKDIR /app

COPY ./build/libs/metrics-writer-timescale-all-1.0-SNAPSHOT.jar app.jar

ENV JAVA_OPTS "-Xmx1024m \
    -Xss256k \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
 "

CMD ["sh", "-c", "java ${JAVA_OPTS} -jar app.jar"]