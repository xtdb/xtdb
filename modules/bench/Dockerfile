FROM eclipse-temurin:21

WORKDIR /opt/xtdb

# Default JVM options - override by setting JDK_JAVA_OPTIONS at runtime
ENV JDK_JAVA_OPTIONS="-Xmx3g -Xms3g"

ENTRYPOINT [ \
    "java","-cp","xtdb-bench.jar", \
    "-Dclojure.main.report=stderr", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--enable-native-access=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-Darrow.memory.debug.allocator=false", \
    "clojure.main", "-m", "xtdb.bench"]

ADD build/libs/bench-standalone.jar xtdb-bench.jar
