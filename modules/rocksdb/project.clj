(defproject com.xtdb/xtdb-rocksdb "<inherited>"
  :description "XTDB RocksDB"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]
                 [com.xtdb/xtdb-metrics :scope "provided"]
                 [org.rocksdb/rocksdbjni "7.3.1"]])
