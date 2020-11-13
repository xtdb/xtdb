FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /usr/local/lib/crux
ENTRYPOINT ["java","-cp","crux-bench-standalone.jar","-Dclojure.main.report=stderr","-Xms3g","-Xmx3g","clojure.main", "-m"]
CMD ["crux.bench.main"]

ADD data/weather_small_locations.csv crux-bench/data/
ADD data/weather_small_conditions.csv crux-bench/data/
ADD data/devices_small_device_info.csv crux-bench/data/
ADD data/devices_small_readings.csv crux-bench/data/

ARG crux_commit='commit-not-found'
ENV COMMIT_HASH=$crux_commit
ENV MALLOC_ARENA_MAX=2
ADD target/crux-bench-standalone.jar .
