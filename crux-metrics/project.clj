(defproject juxt/crux-metrics "derived-from-git"
  :description "Provides Metrics for Crux nodes"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [metrics-clojure "2.10.0"]
                 [com.soundcloud/prometheus-clj "2.4.1"]
                 #_[ring-server "0.5.0"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware])
