FROM clojure:openjdk-11-lein-2.9.0

WORKDIR /usr/local/lib/crux
CMD ["java","-cp","crux-soak.jar","-Xms3g","-Xmx3g","clojure.main", "-m", "crux.soak.main"]

EXPOSE 8080
ADD target/crux-soak.jar .
