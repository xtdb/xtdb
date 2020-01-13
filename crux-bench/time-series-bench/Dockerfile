FROM clojure:openjdk-11-lein-2.9.0

VOLUME /root/.m2

WORKDIR /time-series-bench

CMD ["lein", "run"]

ADD project.clj .
ADD src/ ./src/

ADD https://timescaledata.blob.core.windows.net/datasets/devices_small.tar.gz ./resources/
ADD https://timescaledata.blob.core.windows.net/datasets/weather_small.tar.gz ./resources/
RUN tar -xf resources/devices_small.tar.gz -C resources/
RUN tar -xf resources/weather_small.tar.gz -C resources/
