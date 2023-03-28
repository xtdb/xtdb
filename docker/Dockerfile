FROM eclipse-temurin:17

LABEL org.opencontainers.image.source=https://github.com/xtdb/core2
LABEL org.opencontainers.image.description="XTDB Core2"
LABEL org.opencontainers.image.licenses="The MIT License"

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","core2-standalone.jar", \
    "clojure.main", "-m", "core2.main"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ARG GIT_SHA
ARG CORE2_VERSION
ENV GIT_SHA=${GIT_SHA}
ENV CORE2_VERSION=${CORE2_VERSION}

LABEL org.opencontainers.image.version=${CORE2_VERSION}
LABEL org.opencontainers.image.revision=${GIT_SHA}

ADD core2.edn core2.edn
ADD build/libs/core2-standalone.jar core2-standalone.jar
