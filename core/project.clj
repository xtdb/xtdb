(defproject pro.juxt.crux-labs/core2-core "<inherited>"
  :description "Core2 Initiative"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition
                             :javac-options]}


  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure]
                 [pro.juxt.crux-labs/core2-api]

                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [org.clojure/data.json]
                 [org.clojure/data.csv]
                 [org.clojure/tools.cli "1.0.206"]

                 [org.apache.arrow/arrow-algorithm "4.0.1"]
                 [org.apache.arrow/arrow-compression "4.0.1"]
                 [org.apache.arrow/arrow-vector "4.0.1"]
                 [org.apache.arrow/arrow-memory-netty "4.0.1"]

                 [org.roaringbitmap/RoaringBitmap "0.9.15"]

                 [pro.juxt.clojars-mirrors.integrant/integrant "0.8.0"]]

  :profiles {:dev [:test]
             :test {:dependencies [[pro.juxt.crux-labs/core2-datasets]
                                   [org.clojure/test.check "1.1.0"]

                                   [cheshire "5.10.0"]]

                    :resource-paths ["test-resources"]}}

  :test-selectors {:default (complement (some-fn :integration :timescale))
                   :integration :integration
                   :timescale :timescale}

  :aot [core2.main]

  :java-source-paths ["src"]

  :javac-options ["-source" "11" "-target" "11"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace []
                          :omit-source true
                          :filespecs ^:replace []}})
