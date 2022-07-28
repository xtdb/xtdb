(defproject com.xtdb/xtdb-metrics "<inherited>"
  :description "Provides Metrics for XTDB nodes"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.xtdb/xtdb-core]
                 [io.dropwizard.metrics/metrics-core "4.2.10"]
                 [io.dropwizard.metrics/metrics-jvm "4.2.10"]]

  :exclusions [com.google.errorprone/error_prone_annotations
               org.checkerframework/checker-qual]

  :profiles {:provided {:dependencies
                        [[io.dropwizard.metrics/metrics-jmx "4.2.10"]

                         [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.8"]
                         [software.amazon.awssdk/cloudwatch]

                         [org.dhatim/dropwizard-prometheus "3.1.4"]
                         [io.prometheus/simpleclient_pushgateway "0.16.0"]
                         [io.prometheus/simpleclient_dropwizard "0.16.0"]
                         [io.prometheus/simpleclient_hotspot "0.16.0"] ;; required for prometheus jvm metrics
                         [clj-commons/iapetos "0.1.12"]]}

             :dev {:dependencies
                   [[ch.qos.logback/logback-classic]]}})
