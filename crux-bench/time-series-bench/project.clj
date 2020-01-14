(defproject juxt/crux-bench "0.1.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "20.01-1.6.3-alpha-SNAPSHOT"]
                 [juxt/crux-kafka "20.01-1.6.3-alpha-SNAPSHOT"]
                 [juxt/crux-kafka-embedded "20.01-1.6.3-alpha-SNAPSHOT"]
                 [juxt/crux-rocksdb "20.01-1.6.3-alpha-SNAPSHOT"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :jvm-opts ["-Xms3g" "-Xmx3g" "-Dlogback.configurationFile=logback.xml"]
  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :main ^:skip-aot crux.bench.main)
  :resource-paths ["resources" "data"])
