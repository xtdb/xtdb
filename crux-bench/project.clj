(defproject juxt/crux-bench "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-metrics "derived-from-git"]
                 [juxt/crux-test "derived-from-git"]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [clj-http "3.10.0" :exclusions [org.apache.httpcomponents/httpclient]]
                 [org.apache.httpcomponents/httpclient "4.5.9"]
                 
                 ;; Dependancy resolution
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]]

  :middleware [leiningen.project-version/middleware]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn)
