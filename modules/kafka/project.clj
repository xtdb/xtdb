(defproject com.xtdb.labs/core2-kafka "<inherited>"
  :description "Core2 Kafka integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[com.xtdb.labs/core2-api]
                 [com.xtdb.labs/core2-core]
                 [org.apache.kafka/kafka-clients "2.8.0"]]

  :profiles {:test {:dependencies [[cheshire]]}}

  :test-selectors {:default (complement :requires-docker)
                   :with-docker :requires-docker})
