(defproject juxt/imdb "0.1.0-SNAPSHOT"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux "0.1.0-SNAPSHOT"]
                 [yada "1.3.0-alpha7"]
                 [hiccup "2.0.0-alpha2"]

                 [org.rocksdb/rocksdbjni "5.17.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.kafka/kafka-clients "2.1.0"]

                 [org.clojure/data.csv "0.1.4"]]
  :global-vars {*warn-on-reflection* true}
  :main imdb.main)
