FROM clojure:openjdk-14-tools-deps

CMD ["clojure", "-A:run"]

WORKDIR /opt/webservice/

ADD src/ ./src/
ADD data/ ./data/
ADD resources/ ./resources/
ADD deps.edn .
