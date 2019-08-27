(defproject juxt/crux-uberjar "derived-from-git"
  :description "Crux Uberjar"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [org.slf4j/slf4j-api "1.7.26"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [juxt/crux-rdf "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]]
  :middleware [leiningen.project-version/middleware]
  :graal {:dependencies [[org.clojure/clojure "1.9.0"]
                         [org.slf4j/slf4j-simple "1.7.26"]]
          :clean-targets []
          :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                     "-Dclojure.spec.compile-asserts=false"]
          :aot ^:replace [crux.kv.memdb
                          crux.kv.rocksdb
                          crux.main.graal]
          :main crux.main.graal}
  :aot [crux.main]
  :main crux.main)
