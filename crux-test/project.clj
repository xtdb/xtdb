(defproject pro.juxt.crux/crux-test "crux-git-version"
  :description "Crux Tests Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.crux/crux-jdbc "crux-git-version"]
                 [pro.juxt.crux/crux-http-server "crux-git-version"]
                 [pro.juxt.crux/crux-rocksdb "crux-git-version"]
                 [pro.juxt.crux/crux-lmdb "crux-git-version"]

                 ;; JDBC
                 [com.h2database/h2 "1.4.200"]
                 [com.opentable.components/otj-pg-embedded "0.13.3"]
                 [org.xerial/sqlite-jdbc "3.28.0"]
                 [mysql/mysql-connector-java "8.0.23"]
                 [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8"]

                 ;; TPCH Fixture
                 [io.airlift.tpch/tpch "0.10"]

                 ;; General:
                 [org.clojure/test.check "1.1.0"]

                 [junit/junit "4.12"] ; for `lein junit`

                 ;; dependency conflict resolution
                 [commons-codec "1.15"]
                 [org.tukaani/xz "1.8"]
                 [com.google.protobuf/protobuf-java "3.13.0"]
                 [com.google.guava/guava "30.1.1-jre"]]

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
               "../docs/reference/modules/ROOT/examples/test"
               "../docs/about/modules/ROOT/examples/test"]

  :jvm-opts ["-server" "-Xmx8g" "-Dlogback.configurationFile=test-resources/logback-test.xml"]
  :middleware [leiningen.project-version/middleware]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn
  :plugins [[lein-junit "1.1.9"]]

  :profiles {:dev {:dependencies []}

             :test {:dependencies [[pro.juxt.crux/crux-kafka "crux-git-version"]
                                   [pro.juxt.crux/crux-kafka-connect "crux-git-version"]
                                   [pro.juxt.crux/crux-kafka-embedded "crux-git-version"]
                                   [pro.juxt.crux/crux-http-client "crux-git-version"]
                                   [pro.juxt.crux/crux-metrics "crux-git-version"]
                                   [pro.juxt.crux-labs/crux-rdf "crux-git-version"]
                                   [pro.juxt.crux/crux-sql "crux-git-version"]

                                   ;; Uncomment to test Oracle, you'll need to locally install the JAR:
                                   ;; [com.oracle/ojdbc "19.3.0.0"]

                                   ;; General:
                                   [ch.qos.logback/logback-classic "1.2.3"]
                                   [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]

                                   ;; Outer tests:
                                   [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                                   [criterium "0.4.5"]

                                   ;; Junit Tests
                                   [junit/junit "4.12"]

                                   ;; Authenticated HTTP Server/Client Tests
                                   [com.nimbusds/nimbus-jose-jwt "9.7"]

                                   ;; Kafka connect tests
                                   [org.apache.kafka/connect-api "2.6.0"]]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
