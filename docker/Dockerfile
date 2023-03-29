FROM eclipse-temurin:17

LABEL org.opencontainers.image.source=https://github.com/xtdb/core2
LABEL org.opencontainers.image.description="XTDB 2.x"
LABEL org.opencontainers.image.licenses="The MIT License"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb-standalone.jar", \
    "clojure.main", "-m", "xtdb.main"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ARG GIT_SHA
ARG XTDB_VERSION
ENV GIT_SHA=${GIT_SHA}
ENV XTDB_VERSION=${XTDB_VERSION}

LABEL org.opencontainers.image.version=${XTDB_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}

ADD xtdb.edn xtdb.edn
ADD build/libs/xtdb-standalone.jar xtdb-standalone.jar
