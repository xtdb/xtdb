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

  :dependencies [[org.clojure/clojure]])
