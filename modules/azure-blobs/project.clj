(defproject com.xtdb/xtdb-azure-blobs "<inherited>"
  :description "XTDB Azure Blobs Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [com.xtdb/xtdb-core]
                 [com.azure/azure-storage-blob "12.22.0"]
                 [com.azure/azure-identity "1.9.0"
                  :exclusions [net.minidev/json-smart
                               com.nimbusds/lang-tag]]

                 ;; remove misc dependency warnings
                 [com.azure/azure-core "1.39.0"]
                 [com.azure/azure-core-http-netty "1.13.3"]
                 [net.java.dev.jna/jna "5.13.0"]
                 [net.java.dev.jna/jna-platform "5.13.0"]
                 [io.netty/netty-transport "4.1.89.Final"]
                 [net.minidev/json-smart "2.4.10"]
                 [com.nimbusds/lang-tag "1.4.3"]]

  :profiles {:test {:dependencies [[com.xtdb/xtdb-test]]}}

  :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
