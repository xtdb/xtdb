(defproject pro.juxt.crux-labs/core2-client "<inherited>"
  :description "Core2 Client"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[pro.juxt.crux-labs/core2-api]
                 [hato]
                 [com.cognitect/transit-clj]]

  :profiles {:test {:dependencies [[pro.juxt.crux-labs/core2-core]
                                   [pro.juxt.crux-labs/core2-server]
                                   [cheshire]]}})
