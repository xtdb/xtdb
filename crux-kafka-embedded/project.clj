(defproject juxt/crux-kafka-embedded "crux-git-version-beta"
  :description "Crux Kafka Embedded"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [org.apache.kafka/kafka_2.12 "2.3.0"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.9.9"]
                 [org.apache.zookeeper/zookeeper "3.4.14"
                  :exclusions [io.netty/netty
                               jline
                               org.apache.yetus/audience-annotations
                               org.slf4j/slf4j-log4j12
                               log4j]]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
