(defproject juxt/crux "19.04-1.0.3-alpha-SNAPSHOT"
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.agrona/agrona "0.9.33"]
                 ;; Provided dependencies included in uberjar.
                 [org.clojure/tools.cli "0.4.1" :scope "provided"]
                 [org.apache.kafka/kafka-clients "2.1.1" :scope "provided"]
                 [com.github.jnr/jnr-ffi "2.1.9" :scope "provided"]
                 [org.rocksdb/rocksdbjni "5.17.2" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.1" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.1" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.1" :scope "provided"]
                 [org.lmdbjava/lmdbjava "0.6.3" :exclusions [com.github.jnr/jffi] :scope "provided"]
                 [ring/ring-core "1.7.1" :scope "provided"]
                 [ring/ring-jetty-adapter "1.7.1" :scope "provided"]
                 [ring-cors "0.1.13" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.4.5" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.4.5" :scope "provided"]]
  :profiles { ;; Provided dependencies excluded from uberjar.
             :provided {:dependencies [[org.apache.kafka/kafka_2.11 "2.1.1"]
                                       [org.apache.zookeeper/zookeeper "3.4.13"
                                        :exclusions [io.netty/netty
                                                     jline
                                                     org.apache.yetus/audience-annotations
                                                     org.slf4j/slf4j-log4j12
                                                     log4j]]
                                       [clj-http "3.9.1"]
                                       [http-kit "2.3.0"]]}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                       :resource-paths ["resources-uberjar"]}
             :graal {:dependencies [[org.clojure/clojure "1.9.0"]
                                    [org.slf4j/slf4j-simple "1.7.26"]]
                     :clean-targets []
                     :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                                "-Dclojure.spec.compile-asserts=false"]
                     :aot ^:replace [crux.kv.memdb
                                     crux.kv.rocksdb
                                     crux.main.graal]
                     :main crux.main.graal}
             :dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [criterium "0.4.4"]
                                  [org.clojure/test.check "0.10.0-alpha3"]
                                  [com.datomic/datomic-free "0.9.5697"
                                   :exclusions [org.slf4j/slf4j-nop]]
                                  [org.neo4j/neo4j "3.5.3"
                                   :exclusions [com.github.ben-manes.caffeine/caffeine
                                                io.netty/netty-all
                                                org.ow2.asm/asm
                                                org.ow2.asm/asm-analysis
                                                org.ow2.asm/asm-tree
                                                org.ow2.asm/asm-util]]
                                  [com.github.ben-manes.caffeine/caffeine "2.6.2"]
                                  [org.eclipse.rdf4j/rdf4j-sail-nativerdf "2.4.5"]
                                  [org.eclipse.rdf4j/rdf4j-repository-sail "2.4.5"
                                   :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]
                                  [org.eclipse.rdf4j/rdf4j-repository-sparql "2.4.5"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.ejml/ejml-dsparse "0.37"
                                   :exclusions [com.google.code.findbugs/jsr305]]
                                  [org.roaringbitmap/RoaringBitmap "0.7.42"]
                                  [integrant "0.6.3"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}}}
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"]
  :aot [crux.main]
  :main crux.main
  :global-vars {*warn-on-reflection* true}
  ;; TODO: Leiningen vs CIDER nREPL version issue.
  ;;       https://github.com/technomancy/leiningen/pull/2367
  ;; :pedantic? :abort
  :jvm-opts ~(vec (remove nil?
                          [(when-let [f (System/getenv "YOURKIT_AGENT")]
                             (str "-agentpath:" (clojure.java.io/as-file f)))])))
