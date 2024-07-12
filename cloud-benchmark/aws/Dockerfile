FROM eclipse-temurin:21

LABEL org.opencontainers.image.source=https://github.com/xtdb/xtdb
LABEL org.opencontainers.image.description="XTDB AWS Benchmark 2.x"
LABEL org.opencontainers.image.licenses="MPL-2.0"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "-Xmx1750m", "-Xms1750m", \
    "-XX:MaxDirectMemorySize=1750m", \
    "-XX:MaxMetaspaceSize=500m", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/var/lib/xtdb", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","xtdb-aws-bench.jar", \
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

ADD aws-config.yaml node-config.yaml
ADD build/libs/xtdb-aws-bench.jar xtdb-aws-bench.jar
