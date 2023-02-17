(def xt-version (or (System/getenv "XTDB_VERSION") "dev-SNAPSHOT"))

(defproject com.xtdb/xtdb-dev xt-version
  :url "https://github.com/xtdb/xtdb"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :managed-dependencies
  [[org.clojure/clojure "1.11.1"]
   [org.clojure/data.csv "1.0.1"]
   [org.clojure/data.json "2.4.0"]
   [org.clojure/java.data "1.0.86"]
   [org.clojure/spec.alpha "0.3.218"]
   [org.clojure/tools.cli "1.0.206"]
   [org.clojure/tools.reader "1.3.6"]
   [org.clojure/tools.logging "1.2.4"]

   [com.xtdb/xtdb-core ~xt-version]
   [com.xtdb/xtdb-rocksdb ~xt-version]
   [com.xtdb/xtdb-lmdb ~xt-version]
   [com.xtdb/xtdb-kafka ~xt-version]
   [com.xtdb/xtdb-kafka-connect ~xt-version]
   [com.xtdb/xtdb-kafka-embedded ~xt-version]
   [com.xtdb/xtdb-jdbc ~xt-version]
   [com.xtdb/xtdb-metrics ~xt-version]
   [com.xtdb.labs/xtdb-http-health-check ~xt-version]
   [com.xtdb/xtdb-http-server ~xt-version]
   [com.xtdb/xtdb-http-client ~xt-version]
   [com.xtdb.labs/xtdb-rdf ~xt-version]
   [com.xtdb/xtdb-sql ~xt-version]
   [com.xtdb/xtdb-azure-blobs ~xt-version]
   [com.xtdb/xtdb-google-cloud-storage ~xt-version]
   [com.xtdb/xtdb-lucene ~xt-version]
   [com.xtdb/xtdb-test ~xt-version]
   [com.xtdb/xtdb-bench ~xt-version]
   [com.xtdb/xtdb-replicator ~xt-version]

   ;;
   [com.xtdb/xtdb-s3 ~xt-version]

   [ch.qos.logback/logback-classic "1.2.11"]
   [ch.qos.logback/logback-core "1.2.11"]
   [cljsjs/react "17.0.1-0"]
   [cljsjs/react-dom "17.0.1-0"]
   [com.bhauman/spell-spec "0.1.2"]
   [com.cognitect/transit-clj "1.0.329"]
   [com.fasterxml.jackson.core/jackson-core "2.13.3"]
   [com.fasterxml.jackson.core/jackson-annotations "2.13.3"]
   [com.fasterxml.jackson.core/jackson-databind "2.13.3"]
   [com.fasterxml.jackson.dataformat/jackson-dataformat-yaml "2.13.3"]
   [com.fasterxml.jackson.datatype/jackson-datatype-jdk8 "2.13.3"]
   [com.google.api-client/google-api-client "1.34.1"]
   [com.google.guava/guava "30.1.1-jre" :exclusions [org.checkerframework/checker-qual]]
   [com.google.protobuf/protobuf-java "3.21.4"]
   [com.nimbusds/nimbus-jose-jwt "9.23"]
   [commons-codec "1.15"]
   [commons-io "2.11.0"]
   [commons-logging "1.2"]
   [crypto-random "1.2.1"]
   [expound "0.9.0"]
   [io.netty/netty-all "4.1.77.Final"]
   [io.netty/netty-buffer "4.1.77.Final"]
   [io.netty/netty-codec "4.1.77.Final"]
   [io.netty/netty-codec-http "4.1.77.Final"]
   [io.netty/netty-handler "4.1.77.Final"]
   [io.netty/netty-resolver "4.1.77.Final"]
   [io.netty/netty-transport-native-epoll "4.1.77.Final"]
   [javax.servlet/javax.servlet-api "4.0.1"]
   [joda-time "2.10.14"]
   [org.apache.commons/commons-lang3 "3.12.0"]
   [org.apache.httpcomponents/httpclient "4.5.13"]
   [org.apache.httpcomponents/httpcore "4.4.14"]
   [org.eclipse.jetty/jetty-alpn-server "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-alpn-openjdk8-server "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-http "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-io "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-security "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-server "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-servlet "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-util "9.4.48.v20220622"]
   [org.eclipse.jetty.http2/http2-server "9.4.48.v20220622"]
   [org.eclipse.jetty.websocket/websocket-server "9.4.48.v20220622"]
   [org.eclipse.jetty.websocket/websocket-servlet "9.4.48.v20220622"]
   [org.eclipse.jetty/jetty-server "9.4.48.v20220622"]
   [org.reactivestreams/reactive-streams "1.0.4"]
   [org.tukaani/xz "1.8"]
   [org.slf4j/slf4j-api "1.7.36"]
   [org.slf4j/slf4j-simple "1.7.36"]
   [org.slf4j/jcl-over-slf4j "1.7.36"]
   [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
   [pro.juxt.clojars-mirrors.com.taoensso/nippy "3.1.1-2"]
   [ring/ring-devel "1.9.5"]
   [software.amazon.awssdk/s3 "2.19.21"]
   [software.amazon.awssdk/cloudwatch "2.19.21"]
   [software.amazon.awssdk/s3-transfer-manager "2.19.21"]]

  :dependencies
  [[org.clojure/clojure]
   [com.xtdb/xtdb-core]
   [com.xtdb/xtdb-rocksdb]
   [com.xtdb/xtdb-lmdb]
   [com.xtdb/xtdb-kafka]
   [com.xtdb/xtdb-kafka-connect]
   [com.xtdb/xtdb-kafka-embedded]
   [com.xtdb/xtdb-jdbc]
   [com.xtdb/xtdb-metrics]
   [com.xtdb/xtdb-http-server]
   [com.xtdb.labs/xtdb-rdf]
   [com.xtdb/xtdb-sql]
   [com.xtdb/xtdb-azure-blobs]
   [com.xtdb/xtdb-google-cloud-storage]
   [com.xtdb/xtdb-lucene]
   [com.xtdb/xtdb-test]
   [com.xtdb/xtdb-bench]
   [com.xtdb/xtdb-replicator]
   [com.xtdb/xtdb-s3]
   [com.xtdb.labs/xtdb-http-health-check]

   [org.apache.kafka/connect-api "2.6.0" :scope "provided"]

   [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]

   [integrant "0.8.0"]
   [integrant/repl "0.3.2"]

   ;; metrics dependencies
   ;; JMX Deps
   [io.dropwizard.metrics/metrics-jmx "4.2.10"]

   ;; Cloudwatch Deps
   [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.8"]
   [software.amazon.awssdk/cloudwatch]

   ;; Prometheus Deps
   [org.dhatim/dropwizard-prometheus "3.1.4"]
   [io.prometheus/simpleclient_pushgateway "0.16.0"]
   [io.prometheus/simpleclient_dropwizard "0.16.0"]
   [io.prometheus/simpleclient_hotspot "0.16.0"]
   [clj-commons/iapetos "0.1.12"]

   ;; lucene test dep
   [org.apache.lucene/lucene-analyzers-common "8.11.2"]

   ;; test test dep
   [criterium "0.4.6"]

   ;; xtdb-core test dep
   [clj-commons/fs "1.6.310"]]

  :source-paths ["dev"]

  :test-paths ["docs/reference/modules/ROOT/examples/test"
               "docs/about/modules/ROOT/examples/test"]

  :jvm-opts ["-Dlogback.configurationFile=resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]

  :profiles {:attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}
             :with-s3-tests {:jvm-opts ["-Dxtdb.s3.test-bucket=crux-s3-test"]}
             :with-azure-blobs-tests {:jvm-opts ["-Dxtdb.azure.blobs.test-storage-account=crux-azure-blobs-test-storage-account"
                                                 "-Dxtdb.azure.blobs.test-container=crux-azure-blobs-test-container"]}
             :with-google-cloud-storage-test {:jvm-opts ["-Dxtdb.google.cloud-storage-test.bucket=crux-gcs-test"]}
             :with-chm-add-opens {:jvm-opts ["--add-opens" "java.base/java.util.concurrent=ALL-UNNAMED"]}
             :nvd {:dependencies [[lein-nvd "2.0.0"]]
                   :plugins [[lein-nvd "1.5.0"]]}}
  :nvd {:fail-threshold 11}
  :pedantic? :warn
  :global-vars {*warn-on-reflection* true}

  :repositories {"snapshots" {:url "https://s01.oss.sonatype.org/content/repositories/snapshots"}}

  :deploy-repositories {"releases" {:url "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :username [:gpg :env/sonatype_username]
                                    :password [:gpg :env/sonatype_password]}
                        "snapshots" {:url "https://s01.oss.sonatype.org/content/repositories/snapshots"
                                     :username [:gpg :env/sonatype_username]
                                     :password [:gpg :env/sonatype_password]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]]))
