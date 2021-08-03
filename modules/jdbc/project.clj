(defproject pro.juxt.crux-labs/core2-jdbc "<inherited>"
  :description "Core2 JDBC integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[pro.juxt.crux-labs/core2-core]
                 [org.clojure/java.data "1.0.86"]
                 [pro.juxt.clojars-mirrors.com.github.seancorfield/next.jdbc "1.2.674"]
                 [com.zaxxer/HikariCP "4.0.3"]
                 [org.postgresql/postgresql "42.2.20" :scope "provided"]]

  :profiles {:dev [:test]
             :test {:dependencies [[org.clojure/data.csv "1.0.0"]
                                   [cheshire "5.10.0"]]}})
