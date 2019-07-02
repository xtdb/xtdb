(defproject juxt/crux-kafka :derived-from-git
  :description "Crux Kakfa"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [juxt/crux-core :derived-from-git]
                 [org.apache.kafka/kafka-clients "2.2.0"]]
  :profiles {:dev {:dependencies [[juxt/crux-rdf :derived-from-git]
                                  [juxt/crux-kafka-embedded :derived-from-git]
                                  [ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"])
