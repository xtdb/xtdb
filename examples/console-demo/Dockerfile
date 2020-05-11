FROM clojure:openjdk-11-lein-2.9.0

WORKDIR /usr/local/lib/crux
ENTRYPOINT ["java","-jar","crux-console-demo.jar"]

EXPOSE 3000
ADD target/crux-console-demo.jar .
ADD resources/ .
