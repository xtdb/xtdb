(defproject crux "0.1.0-SNAPSHOT"
  :description "A Clojure library that gives graph query over a K/V store such as RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/tools.cli "0.3.7"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.rocksdb/rocksdbjni "5.14.2"]
                 [cheshire "5.8.0"]
                 [org.lwjgl/lwjgl "3.2.0" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.0" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.2.0" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.0" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.0"]
                 [org.apache.kafka/kafka-clients "2.0.0"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.3.2" :exclusions [commons-io]]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.3.2"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-jetty-adapter "1.6.3"]
                 [http-kit "2.3.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.apache.kafka/kafka_2.11 "2.0.0"
                                   :exclusions [com.fasterxml.jackson.core/jackson-core]]
                                  [org.apache.zookeeper/zookeeper "3.4.13"
                                   :exclusions [io.netty/netty
                                                jline
                                                org.apache.yetus/audience-annotations
                                                org.slf4j/slf4j-log4j12
                                                log4j]]
                                  [criterium "0.4.4"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
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
