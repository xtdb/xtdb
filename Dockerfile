FROM eclipse-temurin:17

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-Xms1g","-Xmx1g", \
    "-XX:MaxDirectMemorySize=2g", \
    "-XX:MaxMetaspaceSize=512m", \
    "-jar","core2-standalone.jar"]

HEALTHCHECK --start-period=15s --timeout=3s \
    CMD curl -f http://localhost:3000/status || exit 1

ADD docker/core2.edn core2.edn
ADD docker/logback.xml logback.xml
ADD target/core2-standalone.jar core2-standalone.jar

RUN mkdir -p /var/lib/xtdb
VOLUME /var/lib/xtdb
