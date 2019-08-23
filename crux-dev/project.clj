(defproject juxt/crux-dev "derived-from-git"
  :description "Crux Dev Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-lmdb "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-kafka-connect "derived-from-git"]
                 [juxt/crux-kafka-embedded "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [juxt/crux-rdf "derived-from-git"]
                 [juxt/crux-decorators "derived-from-git"]
                 [juxt/crux-test "derived-from-git"]]
  :profiles {:dev {:dependencies [;; General:
                                  [org.clojure/tools.namespace "0.2.11"]
                                  ;; Hakan:
                                  [org.ejml/ejml-dsparse "0.38"
                                   :exclusions [com.google.code.findbugs/jsr305]]
                                  ;; Matrix:
                                  [org.roaringbitmap/RoaringBitmap "0.8.2"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}}}
  :test-paths ["../crux-core/test"
               "../crux-rdf/test"
               "../crux-rocksdb/test"
               "../crux-lmdb/test"
               "../crux-kafka-connect/test"
               "../crux-kafka-embedded/test"
               "../crux-kafka/test"
               "../crux-jdbc/test"
               "../crux-http-client/test"
               "../crux-http-server/test"
               "../crux-uberjar/test"
               "../crux-decorators/test"
               "../crux-test/test"]
  :jvm-opts ["-Dlogback.configurationFile=logback-dev.xml"]
  :middleware [leiningen.project-version/middleware]
;;  :pedantic? :abort
  :global-vars {*warn-on-reflection* true})
