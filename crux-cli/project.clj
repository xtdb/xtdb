(defproject juxt/crux-cli "crux-git-version-beta"
  :description "Crux Uberjar"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-http-server "crux-git-version-alpha"]

                 ;; Dependency resolution
                 [org.slf4j/slf4j-api "1.7.29"]]

  :middleware [leiningen.project-version/middleware]
  :aot [crux.main]
  :main crux.main
  :uberjar-name "crux-standalone.jar"
  :pedantic? :warn
  :profiles {:test {:dependencies [[juxt/crux-test "crux-git-version"]

                                   ;; Dependency resolution
                                   [org.ow2.asm/asm "7.1"]
                                   [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                                   [org.eclipse.jetty/jetty-server "9.4.30.v20200611"]
                                   [org.eclipse.jetty/jetty-util "9.4.30.v20200611"]
                                   [org.eclipse.jetty/jetty-http "9.4.30.v20200611"]
                                   [javax.servlet/javax.servlet-api "4.0.1"]]}})
