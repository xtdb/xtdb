FROM eclipse-temurin:21

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"

LABEL usage="Example docker-run options for custom logging configuration: \
--volume .:/config --env JDK_JAVA_OPTIONS='-Dlogback.configurationFile=/config/logback.xml'"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb-standalone.jar", \
    "clojure.main", "-m", "xtdb.main"]

CMD ["-f", "local_config.yaml"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

EXPOSE 5432
EXPOSE 3000
EXPOSE 8080

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ARG GIT_SHA
ARG XTDB_VERSION
ENV GIT_SHA=${GIT_SHA}
ENV XTDB_VERSION=${XTDB_VERSION}

LABEL org.opencontainers.image.version=${XTDB_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}

ADD local_config.yaml local_config.yaml
ADD build/libs/xtdb-standalone.jar xtdb-standalone.jar
