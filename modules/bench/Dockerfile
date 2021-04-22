FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /opt/core2
ENTRYPOINT [ \
    "java","-cp","core2-bench.jar", \
    "-Dclojure.main.report=stderr", \
    "-Xms3g", "-Xmx3g", \
    "clojure.main", "-m"]

CMD ["core2.bench"]

ADD target/core2-bench.jar .
