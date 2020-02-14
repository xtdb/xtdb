(defproject juxt/crux-bench "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-metrics "derived-from-git"]
                 [juxt/crux-test "derived-from-git" :exclusions [org.apache.commons/commons-lang3
                                                                 org.slf4j/jcl-over-slf4j
                                                                 joda-time]]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [clj-http "3.10.0"]
                 [software.amazon.awssdk/s3 "2.10.61"]

                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/slf4j-nop]]

                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0" :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]

                 ;; Dependency resolution
                 [commons-codec "1.12"]
                 [org.eclipse.jetty/jetty-http "9.4.22.v20191022"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [com.google.guava/guava "28.2-jre"]
                 [org.eclipse.jetty/jetty-util "9.4.26.v20200117"]
                 [org.apache.httpcomponents/httpclient "4.5.9"]
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]]

  :middleware [leiningen.project-version/middleware]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms4g" "-Xmx4g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn)
