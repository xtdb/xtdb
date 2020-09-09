(def modules
  ["crux-core"
   "crux-rdf"
   "crux-metrics"
   "crux-rocksdb" "crux-lmdb"
   "crux-jdbc"
   "crux-http-client" "crux-http-server"
   "crux-kafka-embedded" "crux-kafka-connect" "crux-kafka"
   "crux-sql"
   "crux-test"
   "crux-s3"
   "crux-bench"])

(defproject juxt/crux-dev "crux-dev-SNAPSHOT"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :middleware [leiningen.project-version/middleware]

  :plugins [[lein-sub "0.3.0"]]
  :sub ~modules

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [juxt/crux-core "crux-git-version-beta"]
   [juxt/crux-rocksdb "crux-git-version-beta"]
   [juxt/crux-lmdb "crux-git-version-alpha"]
   [juxt/crux-kafka "crux-git-version-beta"]
   [juxt/crux-kafka-connect "crux-git-version-beta"]
   [juxt/crux-kafka-embedded "crux-git-version-beta"]
   [juxt/crux-jdbc "crux-git-version-beta"]
   [juxt/crux-metrics "crux-git-version-alpha"]
   [juxt/crux-http-server "crux-git-version-alpha"]
   [juxt/crux-rdf "crux-git-version-alpha"]
   [juxt/crux-sql "crux-git-version-alpha"]
   [juxt/crux-test "crux-git-version"]
   [juxt/crux-bench "crux-git-version"]

   [org.apache.kafka/connect-api "2.3.0" :scope "provided"]

   [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]
   [com.h2database/h2 "1.4.199"]
   [com.opentable.components/otj-pg-embedded "0.13.1"]
   [org.xerial/sqlite-jdbc "3.28.0"]
   [mysql/mysql-connector-java "8.0.17"]
   [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8"]

   [integrant "0.8.0"]
   [integrant/repl "0.3.1"]

   ;; dependency conflict resolution
   [com.fasterxml.jackson.core/jackson-core "2.10.2"]
   [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
   [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
   [io.netty/netty-transport "4.1.51.Final"]
   [io.netty/netty-transport "4.1.51.Final"]
   [io.netty/netty-codec-http "4.1.51.Final"]
   [com.google.code.findbugs/jsr305 "3.0.2"]
   [org.apache.commons/commons-lang3 "3.9"]
   [org.apache.commons/commons-text "1.7"]
   [org.apache.commons/commons-compress "1.19"]
   [org.javassist/javassist "3.22.0-GA"]
   [org.lz4/lz4-java "1.7.1"]
   [commons-codec "1.12"]
   [joda-time "2.9.9"]
   [org.eclipse.jetty/jetty-util "9.4.22.v20191022"]
   [org.eclipse.jetty/jetty-http "9.4.22.v20191022"]
   [org.tukaani/xz "1.8"]
   [com.github.spotbugs/spotbugs-annotations "3.1.9"]

   ;; crux metrics dependencies
   ;; JMX Deps
   [io.dropwizard.metrics/metrics-jmx "4.1.2"]

   ;; Cloudwatch Deps
   [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
   [software.amazon.awssdk/cloudwatch "2.10.61"]

   ;; Prometheus Deps
   [org.dhatim/dropwizard-prometheus "2.2.0"]
   [io.prometheus/simpleclient_pushgateway "0.8.1"]
   [io.prometheus/simpleclient_dropwizard "0.8.1"]
   [io.prometheus/simpleclient_hotspot "0.8.1"]
   [clj-commons/iapetos "0.1.9"]

   ;; crux-test test dep
   [criterium "0.4.5"]]

  :source-paths ["dev"]

  :test-paths ["crux-core/test"
               "crux-rdf/test"
               "crux-rocksdb/test"
               "crux-lmdb/test"
               "crux-kafka-connect/test"
               "crux-kafka-embedded/test"
               "crux-kafka/test"
               "crux-jdbc/test"
               "crux-http-client/test"
               "crux-http-server/test"
               "crux-metrics/test"
               "crux-s3/test"
               "crux-bench/test"
               "crux-test/test"]

  :jvm-opts ["-Dlogback.configurationFile=resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}

  :aliases {"check" ["sub" "-s" ~(->> modules (remove #{"crux-jdbc"}) (clojure.string/join ":")) "check"]
            "build" ["do" ["sub" "install"] ["sub" "test"]]}

  :profiles {:attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}
             :with-s3-tests {:jvm-opts ["-Dcrux.s3.test-bucket=crux-s3-test"]}}

  :pedantic? :warn)
