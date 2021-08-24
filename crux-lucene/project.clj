(defproject com.xtdb/xtdb-lucene "<inherited>"
  :description "XTDB Lucene integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.xtdb/xtdb-core]

                 [org.apache.lucene/lucene-core "8.9.0"]
                 [org.apache.lucene/lucene-queryparser "8.9.0"]
                 [org.apache.lucene/lucene-analyzers-common "8.9.0" :scope "test"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [com.xtdb/xtdb-test]
                                  [com.xtdb/xtdb-rocksdb]]}}

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"])
