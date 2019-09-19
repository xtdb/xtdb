(defproject juxt/crux-kafka-connect "derived-from-git"
  :description "Crux Kafka Connect"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-http-client "derived-from-git"]
                 [cheshire "5.9.0"]
                 [com.cognitect/transit-clj "0.8.313" :exclusions [org.msgpack/msgpack]]]
  :profiles {:provided {:dependencies [[org.apache.kafka/connect-api "2.3.0"]]}}
  :middleware [leiningen.project-version/middleware]
  :aliases {"package" ["do" ["uberjar"] ["archive"]]}
  :confluent-hub-manifest {:component_types ["sink" "source"]
                           :description "A Kafka Connect plugin for transferring data between Crux nodes and Kafka. Acts as a source for publishing transacations on a node to a Kafka topic, as well as a sink to receive transactions from a Kafka topic and submit them to a node."
                           :documentation_url "https://github.com/juxt/crux/tree/master/crux-kafka-connect"
                           :license [{:name "The MIT License (MIT)"
                                      :url "https://opensource.org/licenses/MIT"}]
                           :logo "assets/crux-logo.svg"
                           :name "crux-kafka-connect"
                           :owner {:name "JUXT"
                                   :type "organization"
                                   :url "https://juxt.pro/index.html"
                                   :username "juxt"}

                           :title "Crux Kafka Connect"
                           :version "derived-from-git"}
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :plugins [[lein-licenses "0.2.2"]])
