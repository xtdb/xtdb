(defproject pro.juxt.crux-labs/core2-kafka "<inherited>"
  :description "Core2 Kafka integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[pro.juxt.crux-labs/core2-core]
                 [org.apache.kafka/kafka-clients "2.8.0"]])
