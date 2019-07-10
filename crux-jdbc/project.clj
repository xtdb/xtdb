(defproject juxt/crux-jdbc :derived-from-git
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [seancorfield/next.jdbc "1.0.1"]
                 [com.taoensso/nippy "2.14.0"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [com.h2database/h2 "1.4.199"]]}}
  :middleware [leiningen.project-version/middleware])
