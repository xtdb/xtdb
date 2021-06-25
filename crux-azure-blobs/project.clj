(defproject juxt/crux-azure-blobs "crux-git-version-alpha"
  :description "Crux Azure Blobs Document Store"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]

                 ;; dependency resolution
                 [commons-codec "1.15"]]

  :profiles {:test {:dependencies [[juxt/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :pedantic? :warn)
