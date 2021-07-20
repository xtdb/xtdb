(defproject pro.juxt.crux-labs/crux-graal "<inherited>"
  :description "Crux Graal"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/slf4j-simple "1.7.26"]
                 [pro.juxt.crux/crux-core]]

  :jvm-opts ["-Dclojure.compiler.direct-linking=true"
             "-Dclojure.spec.compile-asserts=false"]

  :aot [crux.mem-kv crux.rocksdb crux.main.graal]
  :main crux.main.graal)
