FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /usr/local/lib/crux
ENTRYPOINT ["java","-cp","crux-soak.jar","-Xms512m","-Xmx512m","-Dclojure.main.report=stderr","clojure.main"]
ENV MALLOC_ARENA_MAX=2

EXPOSE 8080
ADD target/crux-soak.jar .
