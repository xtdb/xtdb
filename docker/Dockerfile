# Build JRE
FROM eclipse-temurin:21-jdk-alpine AS jlink
WORKDIR /build
COPY gradlew gradlew
COPY gradle/wrapper gradle/wrapper
COPY build-logic/jlink/settings.gradle.kts settings.gradle.kts
COPY build-logic/jlink/build.gradle.kts build.gradle.kts
RUN chmod +x gradlew && ./gradlew buildCustomJre --no-daemon

# Runtime
FROM alpine:3.23.3

ARG VARIANT=standalone
ARG GIT_SHA
ARG XTDB_VERSION

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"
LABEL org.opencontainers.image.version=${XTDB_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}

LABEL usage="Example docker-run options for custom logging configuration: \
--volume .:/config --env JDK_JAVA_OPTIONS='-Dlogback.configurationFile=/config/logback.xml'"

# For healthcheck
RUN apk add --no-cache wget gcompat

COPY --from=jlink /build/build/custom-jre /opt/java
ENV PATH="/opt/java/bin:$PATH"
ENV JAVA_HOME="/opt/java"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--enable-native-access=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb.jar", \
    "clojure.main", "-m", "xtdb.main"]

CMD ["-f", "config.yaml"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD wget -q --spider http://localhost:8080/healthz/alive || exit 1

EXPOSE 5432
EXPOSE 3000
EXPOSE 8080

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ENV GIT_SHA=${GIT_SHA}
ENV XTDB_VERSION=${XTDB_VERSION}

COPY docker/${VARIANT}/*.yaml ./
COPY docker/${VARIANT}/build/libs/xtdb-${VARIANT}.jar xtdb.jar

RUN addgroup -g 20000 xtdb && adduser -u 20000 -G xtdb -D -H xtdb
RUN mkdir -p /home/xtdb && chown -R xtdb:xtdb /home/xtdb
RUN chown -R xtdb:xtdb /var/lib/xtdb

USER xtdb
