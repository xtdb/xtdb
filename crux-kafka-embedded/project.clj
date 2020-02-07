(defproject juxt/crux-kafka-embedded "derived-from-git"
  :description "Crux Kafka Embedded"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [org.apache.kafka/kafka_2.12 "2.4.0"]
                 [org.apache.zookeeper/zookeeper "3.5.6" :exclusions [log4j org.slf4j/slf4j-log4j12]]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
