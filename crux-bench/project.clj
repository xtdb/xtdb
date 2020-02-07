(defproject juxt/crux-bench "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-metrics "derived-from-git"]
                 [juxt/crux-test "derived-from-git" :exclusions [org.apache.commons/commons-lang3
                                                                 commons-codec
                                                                 org.slf4j/jcl-over-slf4j
                                                                 joda-time]]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 [clj-http "3.10.0" :exclusions [org.apache.httpcomponents/httpclient commons-codec]]
                 [amazonica "0.3.152" :exclusions [commons-codec com.google.guava/guava]]

                 [com.datomic/datomic-free "0.9.5697" :exclusions [org.slf4j/slf4j-nop]]

                 ;; rdf
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "3.0.0" :exclusions [commons-codec]]
                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "3.0.0" :exclusions [org.eclipse.rdf4j/rdf4j-http-client commons-codec]]

                 ;; Dependency resolution
                 [org.apache.httpcomponents/httpclient "4.5.9" :exclusions [commons-codec]]
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]]

  :middleware [leiningen.project-version/middleware]

  :resource-paths ["resources" "data"]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-bench-standalone.jar"
  :pedantic? :warn)
