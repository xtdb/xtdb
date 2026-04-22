# Build TSBS generators
FROM golang:1.21-alpine AS tsbs-builder
RUN apk add --no-cache make git
WORKDIR /tsbs
COPY modules/datasets/lib/tsbs /tsbs
RUN make generators

# Runtime image
FROM eclipse-temurin:21

WORKDIR /opt/xtdb

# Default JVM options - override by setting JDK_JAVA_OPTIONS at runtime
ENV JDK_JAVA_OPTIONS="-Xmx3g -Xms3g"

# Copy pre-built TSBS generators
COPY --from=tsbs-builder /tsbs/bin /opt/xtdb/tsbs/bin

ENTRYPOINT [ \
    "java","-cp","xtdb-bench.jar", \
    "-Dclojure.main.report=stderr", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--enable-native-access=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-Darrow.memory.debug.allocator=false", \
    "clojure.main", "-m", "xtdb.bench"]

ADD modules/bench/build/libs/bench-standalone.jar xtdb-bench.jar
