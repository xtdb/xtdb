(defproject juxt/crux-bench "crux-git-version"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.7"]
                 [org.clojure/tools.cli "1.0.194"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-kafka "crux-git-version-beta"]
                 [juxt/crux-kafka-embedded "crux-git-version-beta"]
                 [juxt/crux-rocksdb "crux-git-version-beta"]
                 [juxt/crux-lmdb "crux-git-version-alpha"]
                 [juxt/crux-metrics "crux-git-version-alpha"]
                 [juxt/crux-test "crux-git-version" :exclusions [org.apache.commons/commons-lang3
                                                                 org.slf4j/jcl-over-slf4j
                                                                 joda-time]]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [clj-http "3.10.0"]
                 [software.amazon.awssdk/s3 "2.10.61"]
                 [com.amazonaws/aws-java-sdk-ses "1.11.720"]
                 [com.amazonaws/aws-java-sdk-logs "1.11.720"]
                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/slf4j-nop]]

                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0" :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]
                 ;; neo4j
                 [org.neo4j/neo4j "4.0.0"]

                 ;; cloudwatch metrics deps
                 [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
                 [software.amazon.awssdk/cloudwatch "2.10.61"]

                 ;; Dependency resolution
                 [commons-codec "1.12"]
                 [commons-collections "3.2.2"]
                 [commons-beanutils "1.9.3"]
                 [org.apache.commons/commons-text "1.7"]
                 [io.netty/netty-all "4.1.43.Final"]
                 [org.ow2.asm/asm-util "7.2"]
                 [org.ow2.asm/asm-tree "7.2"]
                 [org.ow2.asm/asm "7.2"]
                 [org.ow2.asm/asm-analysis "7.2"]
                 [com.github.luben/zstd-jni "1.4.3-1"]
                 [com.google.errorprone/error_prone_annotations "2.3.4"]
                 [com.github.ben-manes.caffeine/caffeine "2.8.0"]
                 [org.apache.commons/commons-compress "1.19"]
                 [joda-time "2.9.9"]
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
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn)
