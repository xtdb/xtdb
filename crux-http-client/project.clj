(defproject juxt/crux-http-client "derived-from-git"
  :description "Crux HTTP Client"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clj-http "3.10.0"]
                 [juxt/crux-core "derived-from-git"]
                 [com.nimbusds/nimbus-jose-jwt "8.2.1" :exclusions [net.minidev/json-smart]]
                 [net.minidev/json-smart "2.3"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[juxt/crux-test "derived-from-git" :scope "test"]
                                   [org.clojure/test.check "0.10.0"]
                                   [juxt/crux-http-server "derived-from-git"]]}}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
