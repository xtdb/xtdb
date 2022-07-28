(defproject com.xtdb.labs/xtdb-graal "<inherited>"
  :description "XTDB Graal"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [ch.qos.logback/logback-classic nil :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/slf4j-simple]
                 [com.xtdb/xtdb-core]]

  :jvm-opts ["-Dclojure.compiler.direct-linking=true"
             "-Dclojure.spec.compile-asserts=false"]

  :aot [xtdb.mem-kv xtdb.rocksdb xtdb.main.graal]
  :main xtdb.main.graal)
