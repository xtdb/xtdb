(defproject juxt/crux-bench "0.1.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "20.01-1.6.2-alpha"]
                 [juxt/crux-kafka "20.01-1.6.2-alpha"]
                 [juxt/crux-kafka-embedded "20.01-1.6.2-alpha"]
                 [juxt/crux-rocksdb "20.01-1.6.2-alpha"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :resource-paths ["resources" "data"]
  :main ^:skip-aot crux.bench.main)
