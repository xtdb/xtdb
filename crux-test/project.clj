(defproject juxt/crux-test "derived-from-git"
  :description "Crux Tests Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [juxt/crux-rdf "derived-from-git"]
                 [juxt/crux-decorators "derived-from-git"]

                 ;; JDBC
                 [com.h2database/h2 "1.4.199"]
                 [com.opentable.components/otj-pg-embedded "0.13.1"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [mysql/mysql-connector-java "8.0.17"]

                 ;; Uncomment to test Oracle, you'll need to locally install the JAR:
                 ;; [com.oracle/ojdbc "12.2.0.1"]

                 ;; General:
                 [org.clojure/test.check "0.10.0-alpha3"]
                 [ch.qos.logback/logback-classic "1.2.3"]

                 ;; Outer tests:
                 [org.eclipse.rdf4j/rdf4j-repository-sparql "2.5.1"]
                 [criterium "0.4.5"]

                 ;; Watdiv:
                 [com.datomic/datomic-free "0.9.5697"
                  :exclusions [org.slf4j/slf4j-nop]]
                 [org.neo4j/neo4j "3.5.5"
                  :exclusions [com.github.ben-manes.caffeine/caffeine
                               com.github.luben/zstd-jni
                               io.netty/netty-all
                               org.ow2.asm/asm
                               org.ow2.asm/asm-analysis
                               org.ow2.asm/asm-tree
                               org.ow2.asm/asm-util]]

                 [org.eclipse.rdf4j/rdf4j-sail-nativerdf "2.5.1"]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "2.5.1"
                  :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]]
  :jvm-opts
  ["-server" "-Xmx8g"
   "-Dlogback.configurationFile=logback.xml"]
  :middleware [leiningen.project-version/middleware])
