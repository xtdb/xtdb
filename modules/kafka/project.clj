(defproject com.xtdb/xtdb-kafka "<inherited>"
  :description "XTDB Kafka"

  :plugins [[lein-javadoc "0.3.0"]
            [lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [com.xtdb/xtdb-core]
                 [org.apache.kafka/kafka-clients "2.6.1" :exclusions [org.lz4/lz4-java
                                                                      org.xerial.snappy/snappy-java]]
                 [org.xerial.snappy/snappy-java "1.1.8.4"]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire]
                 [com.cognitect/transit-clj nil :exclusions [org.msgpack/msgpack]]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic]]}}

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :javadoc-opts {:package-names ["xtdb"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "XTDB Kafka Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"
                                   "-link" "https://kafka.apache.org/26/javadoc/"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}})
