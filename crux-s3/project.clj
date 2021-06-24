(defproject pro.juxt.crux/crux-s3 "crux-git-version-beta"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]

                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [software.amazon.awssdk/s3 "2.10.91"]

                 ;; dependency resolution
                 [io.netty/netty-codec "4.1.62.Final"]
                 [io.netty/netty-codec-http "4.1.62.Final"]
                 [io.netty/netty-handler "4.1.62.Final"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :profiles {:test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :pedantic? :warn)
