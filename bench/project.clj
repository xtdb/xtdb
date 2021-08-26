(defproject com.xtdb/xtdb-bench "<inherited>"
  :description "XTDB Benchmarking tools"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.json "2.3.1"]
                 [org.clojure/tools.cli "1.0.206"]
                 [com.xtdb/xtdb-core]
                 [com.xtdb/xtdb-jdbc]
                 [com.xtdb/xtdb-kafka]
                 [com.xtdb/xtdb-kafka-embedded]
                 [com.xtdb/xtdb-rocksdb]
                 [com.xtdb/xtdb-lmdb]
                 [com.xtdb/xtdb-lucene]
                 [com.xtdb/xtdb-metrics]
                 [com.xtdb.labs/xtdb-rdf]
                 [com.xtdb/xtdb-test]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                 [software.amazon.awssdk/s3 "2.16.32"]
                 [com.amazonaws/aws-java-sdk-ses "1.11.988"]
                 [com.amazonaws/aws-java-sdk-logs "1.11.988"]


                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0"]

                 ;; cloudwatch metrics deps
                 [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
                 [software.amazon.awssdk/cloudwatch "2.16.32"]

                 ;; Dependency resolution
                 [io.netty/netty-all "4.1.62.Final"]
                 [io.netty/netty-resolver "4.1.62.Final"]
                 [io.netty/netty-handler "4.1.62.Final"]
                 [io.netty/netty-buffer "4.1.62.Final"]
                 [io.netty/netty-codec-http "4.1.62.Final"]
                 [io.netty/netty-transport-native-epoll "4.1.62.Final"]
                 [com.google.guava/guava "30.1.1-jre"]
                 [org.apache.httpcomponents/httpclient "4.5.13"]
                 [org.apache.httpcomponents/httpcore "4.4.14"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"

  :profiles {:with-neo4j {:dependencies [[org.neo4j/neo4j "4.0.0"]]
                          :source-paths ["src-neo4j"]}
             :with-datomic {:dependencies [[com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/*]]]
                            :source-paths ["src-datomic"]}})
