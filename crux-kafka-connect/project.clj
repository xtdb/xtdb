(defproject juxt/crux-kafka-connect "derived-from-git"
  :description "Crux Kafka Connect"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-http-client "derived-from-git"]
                 [cheshire "5.9.0"]
                 [com.cognitect/transit-clj "0.8.313" :exclusions [org.msgpack/msgpack]]]
  :profiles {:provided {:dependencies [[org.apache.kafka/connect-api "2.3.0"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
