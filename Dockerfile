FROM eclipse-temurin:17

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-jar","core2-standalone.jar"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb

ARG GIT_SHA
ENV GIT_SHA=${GIT_SHA}

ADD docker/core2.edn core2.edn
ADD docker/logback.xml logback.xml
ADD target/core2-standalone.jar core2-standalone.jar
