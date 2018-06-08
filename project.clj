(defproject crux "0.1.0-SNAPSHOT"
  :description "A Clojure library that gives graph query over a K/V store such as RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/tools.cli "0.3.7"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.rocksdb/rocksdbjni "5.13.2"]
                 [org.lwjgl/lwjgl "3.1.6" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.1.6" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.1.6"]
                 [org.apache.kafka/kafka-clients "1.1.0"]
                 [commons-io "2.5"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.3.2"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-jetty-adapter "1.6.3"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.apache.kafka/kafka_2.11 "1.1.0"]
                                  [org.apache.zookeeper/zookeeper "3.4.12"
                                   :exclusions [io.netty/netty
                                                jline
                                                org.apache.yetus/audience-annotations
                                                org.slf4j/slf4j-log4j12
                                                log4j]]
                                  [criterium "0.4.4"]]
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
