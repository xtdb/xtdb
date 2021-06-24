(defproject pro.juxt.crux/crux-kafka "crux-git-version-beta"
  :description "Crux Kafka"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [org.apache.kafka/kafka-clients "2.6.0" :exclusions [org.lz4/lz4-java]]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
                 [com.cognitect/transit-clj "1.0.324" :exclusions [org.msgpack/msgpack]]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn)
