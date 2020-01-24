FROM clojure:openjdk-11-lein-2.9.0

WORKDIR /usr/local/lib/crux
CMD java -cp crux-bench-standalone.jar -Xms3g -Xmx3g clojure.main -m crux.bench.main

ADD data/weather_small_locations.csv crux-bench/data/
ADD data/weather_small_conditions.csv crux-bench/data/
ADD data/devices_small_device_info.csv crux-bench/data/
ADD data/devices_small_readings.csv crux-bench/data/

ADD target/crux-bench-standalone.jar .
