(defproject com.xtdb/xtdb-replicator "<inherited>"
  :description "XTDB Replication Log Exporter"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]

                 [com.xtdb/xtdb-core]
                 [com.cognitect/transit-clj]]

  :profiles {:test {:dependencies [[com.xtdb/xtdb-test]]}}

  :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"])
