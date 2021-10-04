(defproject com.xtdb.labs/core2-client "<inherited>"
  :description "Core2 Client"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[com.xtdb.labs/core2-api]
                 [pro.juxt.clojars-mirrors.hato/hato]
                 [pro.juxt.clojars-mirrors.metosin/reitit-core]
                 [com.cognitect/transit-clj]]

  :profiles {:test {:dependencies [[com.xtdb.labs/core2-core]
                                   [com.xtdb.labs/core2-server]
                                   [cheshire]]}})
