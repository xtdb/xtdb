(defproject juxt/crux :derived-from-git
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.taoensso/encore "2.114.0"]

                 [juxt/crux-core :derived-from-git]
                 [juxt/crux-lmdb :derived-from-git]
                 [juxt/crux-kafka :derived-from-git]
                 [juxt/crux-http-server :derived-from-git]

                 ;; Provided dependencies included in uberjar.
                 [org.clojure/tools.cli "0.4.2" :scope "provided"]

                 [org.rocksdb/rocksdbjni "6.0.1" :scope "provided"]

                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.5.1" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.5.1" :scope "provided"]]
  :middleware [leiningen.project-version/middleware]
  :profiles { ;; Provided dependencies excluded from uberjar.
             :provided {:dependencies [[clj-http "3.10.0"]
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
             :dev {:dependencies [[juxt/crux-dev :derived-from-git]
                                  [org.clojure/tools.namespace "0.2.11"]
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

  :plugins [[lein-sub "0.3.0"]]
  :sub ["crux-kafka" "crux-lmdb" "crux-core" "crux-dev" "crux-http-server"]

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
