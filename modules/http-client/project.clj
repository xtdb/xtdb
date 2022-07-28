(defproject com.xtdb/xtdb-http-client "<inherited>"
  :description "XTDB HTTP Client"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]
                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                 [com.nimbusds/nimbus-jose-jwt]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic]]
                   :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"]}
             :test {:dependencies [[com.xtdb/xtdb-test]
                                   [com.xtdb/xtdb-http-server]]}})
