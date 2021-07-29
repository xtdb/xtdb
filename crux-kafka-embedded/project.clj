(defproject pro.juxt.crux/crux-kafka-embedded "<inherited>"
  :description "Crux Kafka Embedded"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core]
                 [org.apache.kafka/kafka_2.12 "2.6.0"]
                 [org.apache.zookeeper/zookeeper "3.6.1"
                  ;; naughty of ZK to depend on a specific SLF4J impl, we don't want to.
                  :exclusions [org.slf4j/slf4j-log4j12]]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}})
