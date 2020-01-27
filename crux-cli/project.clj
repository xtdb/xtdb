(defproject juxt/crux-cli "derived-from-git"
  :description "Crux Uberjar"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]]

  :middleware [leiningen.project-version/middleware]
  :aot [crux.main]
  :main crux.main

  :pedantic? :warn)
