(defproject pro.juxt.crux/crux-kafka "crux-git-version"
  :description "Crux Kafka"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [org.apache.kafka/kafka-clients "2.6.0" :exclusions [org.lz4/lz4-java]]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
                 [com.cognitect/transit-clj "1.0.324" :exclusions [org.msgpack/msgpack]]]
  :plugins [[lein-javadoc "0.3.0"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn

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
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
