FROM clojure:openjdk-11-lein-2.9.3

WORKDIR /usr/local/lib/xtdb
ENTRYPOINT ["java","-cp","xtdb-bench-standalone.jar","-Dclojure.main.report=stderr","-Xms3g","-Xmx3g","clojure.main", "-m"]
CMD ["xtdb.bench.main"]

ADD data/weather_small_locations.csv bench/data/
ADD data/weather_small_conditions.csv bench/data/
ADD data/devices_small_device_info.csv bench/data/
ADD data/devices_small_readings.csv bench/data/

ARG xtdb_commit='commit-not-found'
ENV COMMIT_HASH=$xtdb_commit
ENV MALLOC_ARENA_MAX=2
ADD target/xtdb-bench-standalone.jar .
