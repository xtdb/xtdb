(defproject juxt/crux-kafka "derived-from-git"
  :description "Crux Kafka"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [juxt/crux-core "derived-from-git"]
                 [org.apache.kafka/kafka-clients "2.4.0"]
                 [cheshire "5.9.0"]
                 [com.cognitect/transit-clj "0.8.319" :exclusions [org.msgpack/msgpack]]]
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn)
