(defproject com.xtdb/xtdb-test "<inherited>"
  :description "XTDB Tests Project"

  :plugins [[lein-junit "1.1.9"]
            [lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]
                 [com.xtdb/xtdb-jdbc]
                 [com.xtdb/xtdb-http-server]
                 [com.xtdb/xtdb-rocksdb]
                 [com.xtdb/xtdb-lmdb]

                 ;; JDBC
                 [com.h2database/h2 "2.2.222"]
                 [com.opentable.components/otj-pg-embedded "0.13.3"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [mysql/mysql-connector-java "8.0.23"]
                 [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8"]

                 ;; TPCH Fixture
                 [io.airlift.tpch/tpch "0.10"]

                 ;; General:
                 [org.clojure/test.check "1.1.0"]

                 [junit/junit "4.12"]   ; for `lein junit`
                 ]

  :java-source-paths ["test"
                      "../docs/reference/modules/ROOT/examples/test"]

  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :junit ["test"
          "../docs/reference/modules/ROOT/examples/test"]

  :junit-formatter :brief

  :test-paths ["test"
               "test-resources"
               "../docs/reference/modules/ROOT/examples/test"
               "../docs/about/modules/ROOT/examples/test"]

  :jvm-opts ["-server" "-Xmx8g" "-Dlogback.configurationFile=test-resources/logback-test.xml"]

  :profiles {:dev {:dependencies []}

             :test {:dependencies [[com.xtdb/xtdb-kafka]
                                   [com.xtdb/xtdb-kafka-connect]
                                   [com.xtdb/xtdb-kafka-embedded]
                                   [com.xtdb/xtdb-http-client]
                                   [com.xtdb/xtdb-metrics]
                                   [com.xtdb.labs/xtdb-rdf]
                                   [com.xtdb/xtdb-sql]

                                   ;; Uncomment to test Oracle, you'll need to locally install the JAR:
                                   ;; [com.oracle/ojdbc "19.3.0.0"]

                                   ;; General:
                                   [ch.qos.logback/logback-classic]
                                   [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]

                                   ;; Outer tests:
                                   [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                                   [criterium "0.4.5"]

                                   ;; Junit Tests
                                   [junit/junit "4.12"]

                                   ;; Authenticated HTTP Server/Client Tests
                                   [com.nimbusds/nimbus-jose-jwt]

                                   ;; Kafka connect tests
                                   [org.apache.kafka/connect-api "2.6.0"]]}}

  ;; empty JARs to keep Maven Central happy
  :classifiers {:sources {:jar-exclusions [#""]}
                :javadoc {:jar-exclusions [#""]}})
