FROM clojure:openjdk-11-lein-2.9.0

VOLUME /root/.m2

WORKDIR /example

ADD project.clj .
ADD src/ ./src/

CMD ["lein", "run"]
