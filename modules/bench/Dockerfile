FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /opt/xtdb
ENTRYPOINT [ \
    "java","-cp","xtdb-bench.jar", \
    "-Dclojure.main.report=stderr", \
    "-Xms3g", "-Xmx3g", \
    "clojure.main", "-m"]

CMD ["xtdb.bench"]

ADD target/xtdb-bench.jar .
