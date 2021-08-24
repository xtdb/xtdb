(defproject com.xtdb/xtdb-azure-blobs "<inherited>"
  :description "XTDB Azure Blobs Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.xtdb/xtdb-core]
                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]

                 ;; dependency resolution
                 [commons-codec "1.15"]]

  :profiles {:test {:dependencies [[com.xtdb/xtdb-test]]}}

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
