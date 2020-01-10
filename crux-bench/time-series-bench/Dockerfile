FROM clojure:openjdk-11-lein-2.9.0

VOLUME /root/.m2

WORKDIR /time-series-bench

ADD https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz ./resources/
ADD project.clj .
ADD src/ ./src/

CMD ["lein", "run"]
