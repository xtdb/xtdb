FROM eclipse-temurin:21

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB Azure Benchmark 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "-Xmx1500m", "-Xms1500m", \
    "-XX:MaxDirectMemorySize=2000m", \
    "-XX:MaxMetaspaceSize=500m", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb-azure-bench.jar", \
    "clojure.main", "-m", "xtdb.run-auctionmark.main"]

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

ADD azure-config.yaml node-config.yaml
ADD build/libs/xtdb-azure-bench.jar xtdb-azure-bench.jar
