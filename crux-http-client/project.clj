(defproject juxt/crux-http-client "crux-git-version-beta"
  :description "Crux HTTP Client"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [clj-http "3.10.1"]
                 [com.nimbusds/nimbus-jose-jwt "8.2.1" :exclusions [net.minidev/json-smart]]
                 [net.minidev/json-smart "2.3"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[juxt/crux-test "crux-git-version"]
                                   [org.clojure/test.check "0.10.0"]
                                   [juxt/crux-http-server "crux-git-version-alpha"]

                                   ;; dependency conflicts
                                   [org.apache.httpcomponents/httpclient "4.5.12"]
                                   [org.apache.httpcomponents/httpcore "4.4.13"]
                                   [org.ow2.asm/asm "7.1"]
                                   [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                                   [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                                   [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
                                   [org.eclipse.jetty/jetty-server "9.4.30.v20200611"]
                                   [org.eclipse.jetty/jetty-util "9.4.30.v20200611"]
                                   [org.eclipse.jetty/jetty-http "9.4.30.v20200611"]
                                   [javax.servlet/javax.servlet-api "4.0.1"]]}}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
