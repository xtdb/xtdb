(defproject pro.juxt.crux/crux-azure-blobs "crux-git-version"
  :description "Crux Azure Blobs Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]

                 ;; dependency resolution
                 [commons-codec "1.15"]]

  :profiles {:test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
