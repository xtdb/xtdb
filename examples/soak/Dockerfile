FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /usr/local/lib/xtdb
ENTRYPOINT ["java","-cp","xtdb-soak.jar","-Xms512m","-Xmx512m","-Dclojure.main.report=stderr","clojure.main"]
ENV MALLOC_ARENA_MAX=2

EXPOSE 8080
ADD target/xtdb-soak.jar .
