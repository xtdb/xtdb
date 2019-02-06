(defproject juxt/crux "0.1.0-SNAPSHOT"
  :description "A Clojure library that gives graph query over a K/V store such as RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.cli "0.4.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.agrona/agrona "0.9.31"]
                 ;; Provided dependencies included in uberjar.
                 [org.apache.kafka/kafka-clients "2.1.0" :scope "provided"]
                 [com.github.jnr/jnr-ffi "2.1.9" :scope "provided"]
                 [org.rocksdb/rocksdbjni "5.17.2" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.1" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.1" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :scope "provided"]
                 [ring/ring-core "1.7.1" :scope "provided"]
                 [ring/ring-jetty-adapter "1.7.1" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.4.3" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.4.3" :scope "provided"]]
  :profiles { ;; Provided dependencies excluded from uberjar.
             :provided {:dependencies [[org.apache.kafka/kafka_2.11 "2.1.0"]
                                       [org.apache.zookeeper/zookeeper "3.4.13"
                                        :exclusions [io.netty/netty
                                                     jline
                                                     org.apache.yetus/audience-annotations
                                                     org.slf4j/slf4j-log4j12
                                                     log4j]]
                                       [clj-http "3.9.1"]
                                       [http-kit "2.3.0"]]}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [criterium "0.4.4"]
                                  [com.datomic/datomic-free "0.9.5697"
                                   :exclusions [org.slf4j/slf4j-nop]]
                                  [org.neo4j/neo4j "3.5.1"
                                   :exclusions [com.github.ben-manes.caffeine/caffeine
                                                io.netty/netty-all
                                                org.ow2.asm/asm
                                                org.ow2.asm/asm-analysis
                                                org.ow2.asm/asm-tree
                                                org.ow2.asm/asm-util]]
                                  [com.github.ben-manes.caffeine/caffeine "2.6.2"]
                                  [org.eclipse.rdf4j/rdf4j-sail-nativerdf "2.4.3"]
                                  [org.eclipse.rdf4j/rdf4j-repository-sail "2.4.3"
                                   :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]
                                  [org.eclipse.rdf4j/rdf4j-repository-sparql "2.4.3"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.ejml/ejml-dsparse "0.37"
                                   :exclusions [com.google.code.findbugs/jsr305]]
                                  [org.roaringbitmap/RoaringBitmap "0.7.36"]
                                  [integrant "0.6.3"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}}}
  :java-source-paths ["src"]
  :javac-options ["-XDignore.symbol.file"]
  :aot [crux.kafka.nippy
        crux.main]
  :main crux.main
  :global-vars {*warn-on-reflection* true}
  ;; TODO: Leiningen vs CIDER nREPL version issue.
  ;;       https://github.com/technomancy/leiningen/pull/2367
  ;; :pedantic? :abort
  :jvm-opts ~(vec (remove nil?
                          [(when-let [f (System/getenv "YOURKIT_AGENT")]
                             (str "-agentpath:" (clojure.java.io/as-file f)))])))
