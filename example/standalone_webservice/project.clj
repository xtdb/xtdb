(defproject juxt/crux-example-standalone-webservice "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux "0.1.0-SNAPSHOT"]

                 [org.apache.kafka/kafka_2.11 "2.1.0"]
                 [org.rocksdb/rocksdbjni "5.17.2"]]
  :main example-standalone-webservice.main
  :jvm-opts ~(vec (remove nil?
                          [(when-let [f (System/getenv "YOURKIT_AGENT")]
                             (str "-agentpath:" (clojure.java.io/as-file f)))])))
