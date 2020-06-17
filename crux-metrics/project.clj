(defproject juxt/crux-metrics "crux-git-version-alpha"
  :description "Provides Metrics for Crux nodes"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [io.dropwizard.metrics/metrics-core "4.1.2"]]

  :profiles {:provided
             {:dependencies
              [[io.dropwizard.metrics/metrics-jmx "4.1.2"]

               [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.5"]
               [software.amazon.awssdk/cloudwatch "2.10.61"]

               [org.dhatim/dropwizard-prometheus "2.2.0"]
               [io.prometheus/simpleclient_pushgateway "0.8.1"]
               [io.prometheus/simpleclient_dropwizard "0.8.1"]
               [io.prometheus/simpleclient_hotspot "0.8.1"] ;; required for prometheus jvm metrics
               [clj-commons/iapetos "0.1.9"]]}
             :dev
             {:dependencies
              [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware])
