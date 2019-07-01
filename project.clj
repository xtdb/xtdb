(defproject juxt/crux :derived-from-git
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.taoensso/encore "2.114.0"]
                 ;; Provided dependencies included in uberjar.
                 [org.clojure/tools.cli "0.4.2" :scope "provided"]
                 [org.apache.kafka/kafka-clients "2.2.0" :scope "provided"]
                 [com.github.jnr/jnr-ffi "2.1.9" :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.0.1" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.2" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.2" :classifier "natives-linux" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl "3.2.2" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.2" :classifier "natives-macos" :native-prefix "" :scope "provided"]
                 [org.lwjgl/lwjgl-lmdb "3.2.2" :scope "provided"]
                 [org.lmdbjava/lmdbjava "0.6.3" :exclusions [com.github.jnr/jffi] :scope "provided"]
                 [ring/ring-core "1.7.1" :scope "provided"]
                 [ring/ring-jetty-adapter "1.7.1" :scope "provided"]
                 [ring/ring-codec "1.1.2" :scope "provided"]
                 [ring-cors "0.1.13" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.5.1" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.5.1" :scope "provided"]]
  :middleware [leiningen.project-version/middleware]
  :profiles { ;; Provided dependencies excluded from uberjar.
             :provided {:dependencies [[org.apache.kafka/kafka_2.12 "2.2.0"]
                                       [org.apache.zookeeper/zookeeper "3.4.14"
                                        :exclusions [io.netty/netty
                                                     jline
                                                     org.apache.yetus/audience-annotations
                                                     org.slf4j/slf4j-log4j12
                                                     log4j]]
                                       [clj-http "3.10.0"]
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
                                  [criterium "0.4.5"]
                                  [com.datomic/datomic-free "0.9.5697"
                                   :exclusions [org.slf4j/slf4j-nop]]
                                  [org.neo4j/neo4j "3.5.5"
                                   :exclusions [com.github.ben-manes.caffeine/caffeine
                                                io.netty/netty-all
                                                org.ow2.asm/asm
                                                org.ow2.asm/asm-analysis
                                                org.ow2.asm/asm-tree
                                                org.ow2.asm/asm-util]]
                                  [com.github.ben-manes.caffeine/caffeine "2.7.0"]
                                  [org.eclipse.rdf4j/rdf4j-sail-nativerdf "2.5.1"]
                                  [org.eclipse.rdf4j/rdf4j-repository-sail "2.5.1"
                                   :exclusions [org.eclipse.rdf4j/rdf4j-http-client]]
                                  [org.eclipse.rdf4j/rdf4j-repository-sparql "2.5.1"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.ejml/ejml-dsparse "0.38"
                                   :exclusions [com.google.code.findbugs/jsr305]]
                                  [org.roaringbitmap/RoaringBitmap "0.8.2"]
                                  [integrant "0.6.3"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}}}

  ; hiera allows to generate ns hierarchy graph for the core
  :hiera {:ignore-ns #{dev hakan prof patrik
                       crux.decorators crux.http-server crux.main.graal}
          :path "dev/ns-hierarchy.png"}

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :aot [crux.main]
  :main crux.main
  :global-vars {*warn-on-reflection* true}
  ;; TODO: Leiningen vs CIDER nREPL version issue.
  ;;       https://github.com/technomancy/leiningen/pull/2367
  ;; :pedantic? :abort
  :jvm-opts ~(vec (remove nil?
                          [(when-let [f (System/getenv "YOURKIT_AGENT")]
                             (str "-agentpath:" (clojure.java.io/as-file f)))]))

  :repositories [["snapshots" {:url "https://repo.clojars.org"
                               :username :env/clojars_username
                               :password :env/clojars_password}]])
