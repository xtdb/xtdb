(defproject juxt/crux-bench "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux "0.1.0-SNAPSHOT"] ; 1
                 [yada "1.3.0-alpha7"]
                 [hiccup "2.0.0-alpha2"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.4.3" ]
                 [org.rocksdb/rocksdbjni "5.17.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.kafka/kafka-clients "2.1.0"]]
  :global-vars {*warn-on-reflection* true}
  :resource-paths ["/watdiv-data/resources" "resources"]
  :main crux-bench.main)
