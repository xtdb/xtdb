(defproject juxt/crux-dev "crux-dev-SNAPSHOT"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :middleware [leiningen.project-version/middleware]

  :dependencies
  [[org.clojure/clojure "1.10.3"]
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
   [juxt/crux-azure-blobs "crux-git-version-alpha"]
   [juxt/crux-google-cloud-storage "crux-git-version-alpha"]
   [juxt/crux-lucene "crux-git-version-alpha"]
   [juxt/crux-test "crux-git-version"]
   [juxt/crux-bench "crux-git-version"]

   [org.apache.kafka/connect-api "2.6.0" :scope "provided"]

   [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]

   [integrant "0.8.0"]
   [integrant/repl "0.3.1"]

   ;; crux metrics dependencies
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

   ;; crux-test test dep
   [criterium "0.4.5"]

   ;; dependency conflict resolution
   [org.apache.commons/commons-lang3 "3.9"]
   [com.google.protobuf/protobuf-java "3.13.0"]
   [joda-time "2.9.9"]]

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
               "crux-azure-blobs/test"
               "crux-google-cloud-storage/test"
               "crux-sql/test"
               "crux-lucene/test"
               "crux-bench/test"
               "crux-test/test"
               "docs/reference/modules/ROOT/examples/test"
               "docs/about/modules/ROOT/examples/test"]

  :jvm-opts ["-Dlogback.configurationFile=resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}

  :profiles {:attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}
             :with-s3-tests {:jvm-opts ["-Dcrux.s3.test-bucket=crux-s3-test"]}
             :with-azure-blobs-tests {:jvm-opts ["-Dcrux.azure.blobs.test-storage-account=crux-azure-blobs-test-storage-account"
                                                 "-Dcrux.azure.blobs.test-container=crux-azure-blobs-test-container"]}
             :with-google-cloud-storage-test {:jvm-opts ["-Dcrux.google.cloud-storage-test.bucket=crux-gcs-test"]}
             :with-chm-add-opens {:jvm-opts ["--add-opens" "java.base/java.util.concurrent=ALL-UNNAMED"]}}

  :pedantic? :warn)
