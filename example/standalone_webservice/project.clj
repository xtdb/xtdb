(defproject juxt/crux-example-standalone-webservice "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux "0.1.0-SNAPSHOT"] ;; 3
                 [yada "1.3.0-alpha7"]
                 [hiccup "2.0.0-alpha2"]
                 [org.rocksdb/rocksdbjni "5.17.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [cljsjs/vega "4.4.0-0"]
                 [cljsjs/vega-lite "3.0.0-rc10-0"]
                 [cljsjs/vega-embed "3.26.0-0"]
                 [cljsjs/codemirror "5.44.0-1"]]
  :global-vars {*warn-on-reflection* true}
  :main example-standalone-webservice.main)
