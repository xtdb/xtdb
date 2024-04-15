FROM eclipse-temurin:21

WORKDIR /usr/local/lib/xtdb

ENTRYPOINT ["java", \
    "-Dclojure.main.report=stderr", \
    "-Dlogback.configurationFile=logback.xml", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-cp","http-proxy.jar", \
    "clojure.main", "-m", "http-proxy.core"]

EXPOSE 3000

RUN mkdir -p /var/lib/xtdb

ADD build/libs/http-proxy.jar http-proxy.jar
