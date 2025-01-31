FROM eclipse-temurin:21

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb-google-cloud.jar", \
    "clojure.main", "-m", "xtdb.main"]

CMD ["-f", "google_cloud_config.yaml"]

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

ADD google_cloud_config.yaml google_cloud_config.yaml
ADD build/libs/xtdb-google-cloud.jar xtdb-google-cloud.jar
