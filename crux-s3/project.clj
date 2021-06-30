(defproject pro.juxt.crux/crux-s3 "crux-git-version"
  :description "Crux S3 integration"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]

                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [software.amazon.awssdk/s3 "2.10.91"]

                 ;; dependency resolution
                 [io.netty/netty-codec "4.1.62.Final"]
                 [io.netty/netty-codec-http "4.1.62.Final"]
                 [io.netty/netty-handler "4.1.62.Final"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :plugins [[lein-javadoc "0.3.0"]]

  :profiles {:test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :pedantic? :warn

  :javadoc-opts {:package-names ["crux"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "Crux S3 Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"
                                   "-link" "https://sdk.amazonaws.com/java/api/latest"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
