(defproject juxt/crux-uberjar "derived-from-git"
  :description "Crux Uberjar"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [juxt/crux-cli "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git" :exclusions [commons-codec]]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git" :exclusions [commons-codec]]
                 [juxt/crux-rdf "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]
                 [juxt/crux-test "derived-from-git" :scope "test"]]
  :middleware [leiningen.project-version/middleware]
  :aot [crux.main]
  :main crux.main

  :profiles {:uberjar-test {:uberjar-name "crux-test-uberjar.jar"}}
  :pedantic? :warn)
