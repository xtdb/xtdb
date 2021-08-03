(defproject pro.juxt.crux-labs/core2-bench "<inherited>"
  :description "Core2 Benchmark suite"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[pro.juxt.crux-labs/core2-core]
                 [pro.juxt.crux-labs/core2-datasets]
                 [pro.juxt.crux-labs/core2-kafka]
                 [pro.juxt.crux-labs/core2-s3]
                 [pro.juxt.crux-labs/core2-jdbc]
                 [org.clojure/data.csv]
                 [ch.qos.logback/logback-classic]]

  :main ^:skip-aot clojure.main
  :uberjar-name "core2-bench.jar")
