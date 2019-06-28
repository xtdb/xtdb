(defproject juxt/imdb "0.1.0-SNAPSHOT"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [juxt/crux "19.06-1.1.0-alpha"]
   [yada "1.3.0-alpha7"]
   [hiccup "2.0.0-alpha2"]

   [org.rocksdb/rocksdbjni "5.17.2"]
   [ch.qos.logback/logback-classic "1.2.3"]
   ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka
   [org.apache.kafka/kafka_2.12 "2.1.1"]
   [org.apache.kafka/kafka-clients "2.1.1"]
   [org.apache.zookeeper/zookeeper "3.4.13"
    :exclusions [io.netty/netty jline org.apache.yetus/audience-annotations
                 org.slf4j/slf4j-log4j12 log4j]]

                 [org.clojure/data.csv "0.1.4"]]
  :global-vars {*warn-on-reflection* true}
  :main imdb.main)
