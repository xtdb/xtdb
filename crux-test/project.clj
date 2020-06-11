(defproject juxt/crux-test "derived-from-git"
  :description "Crux Tests Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git" :exclusions [commons-codec]]
                 [juxt/crux-kafka-connect "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git" :exclusions [commons-codec]]
                 [juxt/crux-metrics "derived-from-git"]
                 [juxt/crux-rdf "derived-from-git"]

                 ;; JDBC
                 [com.zaxxer/HikariCP "3.3.1"]

                 [com.h2database/h2 "1.4.199"]
                 [com.opentable.components/otj-pg-embedded "0.13.1" :exclusions [org.slf4j/slf4j-api
                                                                                 org.tukaani/xz
                                                                                 com.github.spotbugs/spotbugs-annotations
                                                                                 commons-codec]]
                 [org.xerial/sqlite-jdbc "3.28.0"]
                 [mysql/mysql-connector-java "8.0.17"]
                 [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8"]

                 ;; Uncomment to test Oracle, you'll need to locally install the JAR:
                 ;; [com.oracle/ojdbc "12.2.0.1"]

                 ;; General:
                 [org.clojure/test.check "0.10.0"]
                 [org.slf4j/slf4j-api "1.7.29"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.clojure/data.json "0.2.7"]

                 ;; Outer tests:
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0" :exclusions [commons-codec]]
                 [criterium "0.4.5"]

                 ;; Authenticated HTTP Server/Client Tests
                 [com.nimbusds/nimbus-jose-jwt "8.2.1" :exclusions [commons-codec net.minidev/json-smart]]
                 [net.minidev/json-smart "2.3"]

                 ;; TPCH Fixture
                 [io.airlift.tpch/tpch "0.10"]

                 ;; dependency conflict resolution
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
                 [io.netty/netty-transport "4.1.45.Final"]
                 [io.netty/netty-codec-http "4.1.45.Final"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :jvm-opts ["-server" "-Xmx8g" "-Dlogback.configurationFile=test-resources/logback-test.xml"]
  :middleware [leiningen.project-version/middleware]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn

  :profiles {:dev {:dependencies [[circleci/circleci.test "0.4.3"]]}}

  :aliases {"test" ["run" "-m" "circleci.test/dir" :project/test-paths]
            "tests" ["run" "-m" "circleci.test"]
            "retest" ["run" "-m" "circleci.test.retest"]})
