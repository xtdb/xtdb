(defproject pro.juxt.crux-labs/core2-api "<inherited>"
  :description "Core2 API"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition
                             :javac-options]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure]
                 [com.cognitect/transit-clj nil :scope "provided"]]

  :java-source-paths ["src"]

  :profiles {:test {:dependencies [[pro.juxt.crux-labs/core2-core nil :scope "test"]
                                   [pro.juxt.crux-labs/core2-server nil :scope "test"]
                                   [pro.juxt.crux-labs/core2-client nil :scope "test"]
                                   [cheshire nil :scope "test"]]}})
