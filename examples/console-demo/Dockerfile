FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /usr/local/lib/xtdb
ENTRYPOINT ["java","-jar","xtdb-console-demo.jar"]

EXPOSE 3000
ADD target/xtdb-console-demo.jar .
ADD resources/ .
