(defproject pro.juxt.crux/crux-rocksdb "crux-git-version"
  :description "Crux RocksDB"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.crux/crux-metrics "crux-git-version" :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.12.7"]
                 [com.github.jnr/jnr-ffi "2.1.12"]]

  :middleware [leiningen.project-version/middleware])
