(defproject juxt/crux-s3 "crux-git-version-beta"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]

                 [juxt/crux-core "crux-git-version-beta"]
                 [software.amazon.awssdk/s3 "2.10.61"]

                 ;; dependency resolution
                 [io.netty/netty-codec "4.1.45.Final"]
                 [io.netty/netty-codec-http "4.1.45.Final"]
                 [io.netty/netty-handler "4.1.45.Final"]
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

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
