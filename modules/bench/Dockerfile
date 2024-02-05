FROM clojure:openjdk-17-tools-deps-bullseye

WORKDIR /opt/xtdb
ENTRYPOINT [ \
    "java","-cp","bench-dev-SNAPSHOT-all.jar", \
    "-Dclojure.main.report=stderr", \
    "-Xms3g", "-Xmx3g", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "clojure.main", "-m"]

CMD ["xtdb.bench.tpch"]

ADD build/libs/bench-dev-SNAPSHOT-all.jar .
