(defproject juxt/crux-kafka-embedded "derived-from-git"
  :description "Crux Kakfa Embedded"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]

                 [juxt/crux-core "derived-from-git"]

                 [org.apache.kafka/kafka_2.12 "2.2.0"]
                 [org.apache.zookeeper/zookeeper "3.4.14"
                  :exclusions [io.netty/netty
                               jline
                               org.apache.yetus/audience-annotations
                               org.slf4j/slf4j-log4j12
                               log4j]]]
  :middleware [leiningen.project-version/middleware])
