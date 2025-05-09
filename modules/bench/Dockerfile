FROM eclipse-temurin:21

WORKDIR /opt/xtdb
ENTRYPOINT [ \
    "java","-cp","xtdb-bench.jar", \
    "-Dclojure.main.report=stderr", \
    "-Xms3g", "-Xmx3g", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-Dio.netty.tryReflectionSetAccessible=true", \
    "-Darrow.memory.debug.allocator=false", \
    "clojure.main", "-m", "xtdb.bench"]

ADD build/libs/bench-standalone.jar xtdb-bench.jar
ADD cloud/config/aws.yaml config/aws.yaml
ADD cloud/config/azure.yaml config/azure.yaml
ADD cloud/config/google-cloud.yaml config/google-cloud.yaml
