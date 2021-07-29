(defproject pro.juxt.crux/crux-rocksdb "<inherited>"
  :description "Crux RocksDB"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core]
                 [pro.juxt.crux/crux-metrics :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.12.7"]
                 [com.github.jnr/jnr-ffi "2.1.12"]])
