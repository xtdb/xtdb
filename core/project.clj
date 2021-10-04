(defproject com.xtdb.labs/core2-core "<inherited>"
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
                 [com.xtdb.labs/core2-api]

                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [org.clojure/data.json]
                 [org.clojure/data.csv]
                 [org.clojure/tools.cli "1.0.206"]

                 [org.apache.arrow/arrow-algorithm "5.0.0"]
                 [org.apache.arrow/arrow-compression "5.0.0"]
                 [org.apache.arrow/arrow-vector "5.0.0"]
                 [org.apache.arrow/arrow-memory-netty "5.0.0"]

                 [org.roaringbitmap/RoaringBitmap "0.9.15"]

                 [instaparse "1.4.10"]

                 [pro.juxt.clojars-mirrors.integrant/integrant "0.8.0"]]

  :java-source-paths ["src"]

  :profiles {:dev [:test]
             :test {:dependencies [[com.xtdb.labs/core2-datasets]
                                   [cheshire]
                                   [ch.qos.logback/logback-classic]]

                    :resource-paths ["test-resources"]}}

  :aliases {"generate-sql-parser" ["do" ["run" "-m" "core2.sql.parser-generator"] "javac"]}

  :test-selectors {:default (complement (some-fn :integration :timescale))
                   :integration :integration
                   :timescale :timescale}

  :aot [core2.main]

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace []
                          :omit-source true
                          :filespecs ^:replace []}})
