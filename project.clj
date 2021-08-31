(def xt-version (or (System/getenv "XTDB_VERSION") "dev-SNAPSHOT"))

(defproject com.xtdb/xtdb-dev xt-version
  :url "https://github.com/xtdb/xtdb"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :managed-dependencies
  [[com.xtdb/xtdb-core ~xt-version]
   [com.xtdb/xtdb-rocksdb ~xt-version]
   [com.xtdb/xtdb-lmdb ~xt-version]
   [com.xtdb/xtdb-kafka ~xt-version]
   [com.xtdb/xtdb-kafka-connect ~xt-version]
   [com.xtdb/xtdb-kafka-embedded ~xt-version]
   [com.xtdb/xtdb-jdbc ~xt-version]
   [com.xtdb/xtdb-metrics ~xt-version]
   [com.xtdb/xtdb-http-server ~xt-version]
   [com.xtdb/xtdb-http-client ~xt-version]
   [com.xtdb.labs/xtdb-rdf ~xt-version]
   [com.xtdb/xtdb-sql ~xt-version]
   [com.xtdb/xtdb-azure-blobs ~xt-version]
   [com.xtdb/xtdb-google-cloud-storage ~xt-version]
   [com.xtdb/xtdb-lucene ~xt-version]
   [com.xtdb/xtdb-test ~xt-version]
   [com.xtdb/xtdb-bench ~xt-version]]

  :dependencies
  [[org.clojure/clojure "1.10.3"]
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

   [org.apache.kafka/connect-api "2.6.0" :scope "provided"]

   [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]

   [integrant "0.8.0"]
   [integrant/repl "0.3.1"]

   ;; metrics dependencies
   ;; JMX Deps
   [io.dropwizard.metrics/metrics-jmx "4.1.2"]

   ;; Cloudwatch Deps
   [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
   [software.amazon.awssdk/cloudwatch "2.16.32"]

   ;; Prometheus Deps
   [org.dhatim/dropwizard-prometheus "2.2.0"]
   [io.prometheus/simpleclient_pushgateway "0.8.1"]
   [io.prometheus/simpleclient_dropwizard "0.8.1"]
   [io.prometheus/simpleclient_hotspot "0.8.1"]
   [clj-commons/iapetos "0.1.9"]

   ;; lucene test dep
   [org.apache.lucene/lucene-analyzers-common "8.9.0"]

   ;; test test dep
   [criterium "0.4.5"]

   ;; dependency conflict resolution
   [org.apache.commons/commons-lang3 "3.9"]
   [commons-io "2.8.0"]
   [com.google.protobuf/protobuf-java "3.13.0"]
   [joda-time "2.9.9"]]

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
             :with-chm-add-opens {:jvm-opts ["--add-opens" "java.base/java.util.concurrent=ALL-UNNAMED"]}}

  :pedantic? :warn
  :global-vars {*warn-on-reflection* true}

  :repositories {"snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"}}

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :username [:gpg :env/sonatype_username]
                                    :password [:gpg :env/sonatype_password]}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :username [:gpg :env/sonatype_username]
                                     :password [:gpg :env/sonatype_password]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]]))
