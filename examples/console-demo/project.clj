(defproject juxt/console-demo "<inherited>"
  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.xtdb/xtdb-core]
                 [com.xtdb/xtdb-rocksdb]
                 [com.xtdb/xtdb-http-server]
                 [com.xtdb/xtdb-metrics]
                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]]

  :uberjar-name "crux-console-demo.jar"
  :aot [crux.console-demo.main]
  :main crux.console-demo.main
  :pedantic? :warn)
