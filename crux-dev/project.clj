(defproject juxt/crux-dev :derived-from-git
  :description "Crux Dev Project"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-rocksdb :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-lmdb :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-kafka :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-kafka-embedded :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-http-server :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-rdf :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-decorators :derived-from-git :exclusions [org.clojure/tools.reader]]
                 [juxt/crux-test :derived-from-git :exclusions [org.clojure/tools.reader com.github.luben/zstd-jni]]]
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
               "../crux-kafka-embedded/test"
               "../crux-kafka/test"
               "../crux-http-client/test"
               "../crux-http-server/test"
               "../crux-uberjar/test"
               "../crux-decorators/test"
               "../crux-test/test"]
  :jvm-opts ["-Dlogback.configurationFile=logback-dev.xml"]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :abort
  :global-vars {*warn-on-reflection* true})
