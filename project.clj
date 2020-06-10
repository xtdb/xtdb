(def modules
  ["crux-core"
   "crux-rdf"
   "crux-metrics"
   "crux-rocksdb" "crux-lmdb"
   "crux-jdbc"
   "crux-http-client"
   "crux-kafka-embedded" "crux-kafka-connect" "crux-kafka"
   "crux-test"
   "crux-s3"
   "crux-cli"
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
   [juxt/crux-core "derived-from-git"]
   [juxt/crux-rocksdb "derived-from-git"]
   [juxt/crux-lmdb "derived-from-git"]
   [juxt/crux-kafka "derived-from-git"]
   [juxt/crux-kafka-connect "derived-from-git"]
   [juxt/crux-kafka-embedded "derived-from-git"]
   [juxt/crux-jdbc "derived-from-git"]
   [juxt/crux-metrics "derived-from-git"]
   [juxt/crux-http-server "derived-from-git" :exclusions [commons-codec]]
   [juxt/crux-rdf "derived-from-git"]
   [juxt/crux-test "derived-from-git"]
   [juxt/crux-bench "derived-from-git" :exclusions [org.apache.commons/commons-lang3]]
   [juxt/crux-cli "derived-from-git"]

   [org.apache.kafka/connect-api "2.3.0" :scope "provided"]
   [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]

   [integrant "0.8.0"]
   [integrant/repl "0.3.1"]

   ;; dependency conflict resolution
   [com.fasterxml.jackson.core/jackson-core "2.10.2"]
   [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
   [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
   [io.netty/netty-transport "4.1.45.Final"]
   [io.netty/netty-codec-http "4.1.45.Final"]
   [org.reactivestreams/reactive-streams "1.0.3"]

   ;; crux metrics dependencies
   ;; tag::MetricsJMXDeps[]
   [io.dropwizard.metrics/metrics-jmx "4.1.2"]
   ;; end::MetricsJMXDeps[]

   ;; tag::MetricsCloudwatchDeps[]
   [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
   [software.amazon.awssdk/cloudwatch "2.10.61"]
   ;; end::MetricsCloudwatchDeps[]

   ;; tag::MetricsPrometheusDeps[]
   [org.dhatim/dropwizard-prometheus "2.2.0"]
   [io.prometheus/simpleclient_pushgateway "0.8.1"]
   [io.prometheus/simpleclient_dropwizard "0.8.1"]
   [io.prometheus/simpleclient_hotspot "0.8.1"]
   [clj-commons/iapetos "0.1.9"]
   ;; end::MetricsPrometheusDeps[]
   ]

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
