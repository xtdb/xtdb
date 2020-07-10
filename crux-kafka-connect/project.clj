(defproject juxt/crux-kafka-connect "crux-git-version-beta"
  :description "Crux Kafka Connect"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.0.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-http-client "crux-git-version-beta"]
                 [cheshire "5.9.0"]
                 [com.taoensso/nippy "2.14.0" :exclusions [org.tukaani/xz]]
                 ^:source-dep [org.tukaani/xz "1.6"]
                 [com.cognitect/transit-clj "1.0.324" :exclusions [org.msgpack/msgpack]]]
  :profiles {:provided {:dependencies [[org.apache.kafka/connect-api "2.3.0"]]}}
  :middleware [leiningen.project-version/middleware]
  :aliases {"package" ["do" ["inline-deps"] ["with-profile" "+plugin.mranderson/config" "uberjar"] ["archive"]]}
  :confluent-hub-manifest {:component_types ["sink" "source"]
                           :description "A Kafka Connect plugin for transferring data between Crux nodes and Kafka. Acts as a source for publishing transacations on a node to a Kafka topic, as well as a sink to receive transactions from a Kafka topic and submit them to a node."
                           :documentation_url "https://github.com/juxt/crux/tree/master/crux-kafka-connect"
                           :features {:confluent_control_center_integration true,
                                      :delivery_guarantee ["exactly_once"],
                                      :kafka_connect_api true,
                                      :single_message_transforms true,
                                      :supported_encodings ["any"]}
                           :license [{:name "The MIT License (MIT)"
                                      :url "https://opensource.org/licenses/MIT"}]
                           :logo "assets/crux-logo.svg"
                           :name "kafka-connect-crux"
                           :owner {:name "JUXT"
                                   :type "organization"
                                   :url "https://juxt.pro/index.html"
                                   :username "juxt"}

                           :title "Kafka Connect Crux"
                           :tags ["Database"
                                  "Crux"]

                           :version "crux-git-version-beta"}
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :plugins [[lein-licenses "0.2.2"]
            [thomasa/mranderson "0.5.1"]]
  :mranderson {:unresolved-tree true}
  :pedantic? :warn)
