(defproject com.xtdb/xtdb-lucene "<inherited>"
  :description "XTDB Lucene integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]

                 [org.apache.lucene/lucene-core "8.11.2"]
                 [org.apache.lucene/lucene-queryparser "8.11.2"]
                 [org.apache.lucene/lucene-analyzers-common "8.11.2" :scope "test"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic]
                                  [com.xtdb/xtdb-test]
                                  [com.xtdb/xtdb-rocksdb]]}}

  :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"])
