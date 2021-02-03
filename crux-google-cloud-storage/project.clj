(defproject juxt/crux-google-cloud-storage "crux-git-version-alpha"
  :description "Crux Google Cloud Storage Document Store"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [com.google.cloud/google-cloud-nio "0.122.4"]

                 ;; dep resolution
                 [com.google.api-client/google-api-client "1.31.1"]]

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
