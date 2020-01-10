(defproject time-series-bench "0.1.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.5.0"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "19.12-1.6.1-alpha"]
                 [juxt/crux-kafka "19.12-1.6.1-alpha"]
                 [juxt/crux-kafka-embedded "19.12-1.6.1-alpha"]
                 [juxt/crux-rocksdb "19.12-1.6.1-alpha"]]
  :main ^:skip-aot time-series-bench.ts-devices
  :jvm-opts ["-Dlogback.configurationFile=logback.xml"])
