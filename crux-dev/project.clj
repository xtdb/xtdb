(defproject juxt/crux-dev :derived-from-git
  :description "Crux Dev Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core :derived-from-git]
                 [juxt/crux-rocksdb :derived-from-git]
                 [juxt/crux-lmdb :derived-from-git]
                 [juxt/crux-kafka :derived-from-git]
                 [juxt/crux-kafka-embedded :derived-from-git]
                 [juxt/crux-http-server :derived-from-git]
                 [juxt/crux-rdf :derived-from-git]
                 [juxt/crux-decorators :derived-from-git]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.10.0-alpha3"]
                                  [ch.qos.logback/logback-classic "1.2.3"]

                                  ;; Outer tests:
                                  [org.eclipse.rdf4j/rdf4j-repository-sparql "2.5.1"]
                                  [criterium "0.4.5"]

                                  ;; Watdiv:
                                  [com.datomic/datomic-free "0.9.5697"
                                   :exclusions [org.slf4j/slf4j-nop]]
                                  [org.neo4j/neo4j "3.5.5"
                                   :exclusions [com.github.ben-manes.caffeine/caffeine
                                                io.netty/netty-all
                                                org.ow2.asm/asm
                                                org.ow2.asm/asm-analysis
                                                org.ow2.asm/asm-tree
                                                org.ow2.asm/asm-util]]

                                  [org.eclipse.rdf4j/rdf4j-sail-nativerdf "2.5.1"]
                                  [org.eclipse.rdf4j/rdf4j-repository-sail "2.5.1"
                                   :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]]}}
  :test-paths ["../crux-core/test"
               "../crux-rdf/test"
               "../crux-rocksdb/test"
               "../crux-lmdb/test"
               "../crux-kafka-embedded/test"
               "../crux-kafka/test"
               "../crux-http-client/test"
               "../crux-http-server/test"
               "../crux-uberjar/test"
               "../crux-decorators/test"
               "../test"]
  :jvm-opts ["-Dlogback.configurationFile=logback.xml"]
  :middleware [leiningen.project-version/middleware])
