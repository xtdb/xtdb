(defproject pro.juxt.crux/crux-bench "crux-git-version"
  :description "Crux Benchmarking tools"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.json "2.3.1"]
                 [org.clojure/tools.cli "1.0.206"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.crux/crux-jdbc "crux-git-version"]
                 [pro.juxt.crux/crux-kafka "crux-git-version"]
                 [pro.juxt.crux/crux-kafka-embedded "crux-git-version"]
                 [pro.juxt.crux/crux-rocksdb "crux-git-version"]
                 [pro.juxt.crux/crux-lmdb "crux-git-version"]
                 [pro.juxt.crux/crux-metrics "crux-git-version"]
                 [pro.juxt.crux-labs/crux-rdf "crux-git-version"]
                 [pro.juxt.crux/crux-test "crux-git-version"]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                 [software.amazon.awssdk/s3 "2.16.32"]
                 [com.amazonaws/aws-java-sdk-ses "1.11.988"]
                 [com.amazonaws/aws-java-sdk-logs "1.11.988"]
                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/*]]

                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0"]

                 ;; cloudwatch metrics deps
                 [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
                 [software.amazon.awssdk/cloudwatch "2.16.32"]

                 ;; Dependency resolution
                 [io.netty/netty-all "4.1.62.Final"]
                 [io.netty/netty-resolver "4.1.62.Final"]
                 [io.netty/netty-handler "4.1.62.Final"]
                 [io.netty/netty-buffer "4.1.62.Final"]
                 [io.netty/netty-codec-http "4.1.62.Final"]
                 [io.netty/netty-transport-native-epoll "4.1.62.Final"]
                 [com.google.guava/guava "30.1.1-jre"]
                 [org.apache.httpcomponents/httpclient "4.5.13"]
                 [org.apache.httpcomponents/httpcore "4.4.14"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :middleware [leiningen.project-version/middleware]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn

  :profiles {:uberjar [:with-neo4j]
             :with-neo4j {:dependencies [[org.neo4j/neo4j "4.0.0"]]
                          :source-paths ["src-neo4j"]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
