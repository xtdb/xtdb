FROM clojure:openjdk-11-lein-2.9.0

WORKDIR /usr/local/lib/crux

ADD data/weather_small_locations.csv crux-bench/data/
ADD data/weather_small_conditions.csv crux-bench/data/
ADD data/devices_small_device_info.csv crux-bench/data/
ADD data/devices_small_readings.csv crux-bench/data/

ADD . .
RUN lein sub -s "crux-core/:crux-kafka/:crux-kafka-embedded/:crux-rocksdb/:crux-bench/" install
CMD cd crux-bench/ && lein run