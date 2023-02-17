(defproject com.xtdb.labs/xtdb-http-health-check "<inherited>"
  :description "XTDB health check server"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]
                 [pro.juxt.clojars-mirrors.xtdb/xtdb-http-server-deps "0.0.2"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic]]}}

  :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"])
