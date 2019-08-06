(defproject juxt/crux-bench "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux "19.07-1.1.1-alpha"]
                 [yada "1.3.0-alpha10"]
                 [hiccup "2.0.0-alpha2"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.4.3"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.4.3"]
                 [org.rocksdb/rocksdbjni "5.17.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [buddy/buddy-hashers "1.3.0"]

                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/slf4j-nop]]

                 [amazonica "0.3.139"]]
  :global-vars {*warn-on-reflection* true}
  :resource-paths ["resources"
                   #_"/watdiv-data/resources"]
  :uberjar-name "crux-bench-uberjar.jar"
  :jar-name "crux-bench.jar"
  :aot [crux-bench.main]
  :main crux-bench.main)
