(defproject com.xtdb/xtdb-kafka "<inherited>"
  :description "XTDB Kafka"

  :plugins [[lein-javadoc "0.3.0"]
            [lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.xtdb/xtdb-core]
                 [org.apache.kafka/kafka-clients "2.6.0" :exclusions [org.lz4/lz4-java]]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
                 [com.cognitect/transit-clj "1.0.324" :exclusions [org.msgpack/msgpack]]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :javadoc-opts {:package-names ["crux"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "Crux Kafka Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"
                                   "-link" "https://kafka.apache.org/26/javadoc/"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}})
