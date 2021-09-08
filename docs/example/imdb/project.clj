(defproject juxt/imdb "0.1.0-SNAPSHOT"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies
  [[org.clojure/clojure "1.10.3"]
   [juxt/crux-core "19.09-1.4.0-alpha"]
   [juxt/xtdb-rocksdb "19.09-1.4.0-alpha"]
   [juxt/crux-kafka "19.09-1.4.0-alpha"]
   [juxt/crux-kafka-embedded "19.09-1.4.0-alpha"]
   [yada "1.3.0-alpha7"]
   [hiccup "2.0.0-alpha2"]
   [org.clojure/data.csv "0.1.4"]

   [ch.qos.logback/logback-classic "1.2.3"]]
  :global-vars {*warn-on-reflection* true}
  :main imdb.main
  :pedantic? :warn)
