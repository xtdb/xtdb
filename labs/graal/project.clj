(defproject com.xtdb.labs/xtdb-graal "<inherited>"
  :description "XTDB Graal"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/slf4j-simple "1.7.26"]
                 [com.xtdb/xtdb-core]]

  :jvm-opts ["-Dclojure.compiler.direct-linking=true"
             "-Dclojure.spec.compile-asserts=false"]

  :aot [crux.mem-kv xtdb.rocksdb xtdb.main.graal]
  :main xtdb.main.graal)
