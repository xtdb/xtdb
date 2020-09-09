(defproject juxt/crux-bench "crux-git-version"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "1.0.0"]
                 [org.clojure/tools.cli "1.0.194"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-jdbc "crux-git-version-beta"]
                 [juxt/crux-kafka "crux-git-version-beta"]
                 [juxt/crux-kafka-embedded "crux-git-version-beta"]
                 [juxt/crux-rocksdb "crux-git-version-beta"]
                 [juxt/crux-lmdb "crux-git-version-alpha"]
                 [juxt/crux-metrics "crux-git-version-alpha"]
                 [juxt/crux-rdf "crux-git-version-alpha"]
                 [juxt/crux-test "crux-git-version"]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [clj-http "3.10.0"]
                 [software.amazon.awssdk/s3 "2.10.61"]
                 [com.amazonaws/aws-java-sdk-ses "1.11.720"]
                 [com.amazonaws/aws-java-sdk-logs "1.11.720"]
                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/*]]

                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0"]

                 ;; neo4j
                 [org.neo4j/neo4j "4.0.0"]

                 ;; cloudwatch metrics deps
                 [io.github.azagniotov/dropwizard-metrics-cloudwatch "2.0.3"]
                 [software.amazon.awssdk/cloudwatch "2.10.61"]

                 ;; Dependency resolution
                 [commons-collections "3.2.2"]
                 [commons-beanutils "1.9.3"]
                 [org.apache.commons/commons-compress "1.19"]
                 [io.netty/netty-all "4.1.51.Final"]
                 [io.netty/netty-resolver "4.1.51.Final"]
                 [io.netty/netty-handler "4.1.51.Final"]
                 [io.netty/netty-buffer "4.1.51.Final"]
                 [io.netty/netty-codec-http "4.1.51.Final"]
                 [org.ow2.asm/asm-util "7.2"]
                 [org.ow2.asm/asm-tree "7.2"]
                 [org.ow2.asm/asm "7.2"]
                 [org.ow2.asm/asm-analysis "7.2"]
                 [com.github.luben/zstd-jni "1.4.3-1"]
                 [com.google.guava/guava "28.2-jre"]
                 [org.apache.httpcomponents/httpclient "4.5.12"]
                 [org.apache.httpcomponents/httpcore "4.4.13"]
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
                 [com.github.spotbugs/spotbugs-annotations "3.1.9"]
                 [org.reactivestreams/reactive-streams "1.0.3"]
                 [org.codehaus.janino/commons-compiler "3.0.11"]]

  :middleware [leiningen.project-version/middleware]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn)
