(defproject juxt/crux-decorators "derived-from-git"
  :description "Crux Decorators"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware])
