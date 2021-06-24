(defproject pro.juxt.crux/crux-sql "crux-git-version-alpha"
  :description "SQL for Crux using Apache Calcite"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
                 [org.apache.calcite/calcite-core "1.22.0" :exclusions [com.google.code.findbugs/jsr305]]
                 [org.apache.calcite.avatica/avatica-server "1.16.0"]

                 ;; dependency conflict resolution:
                 [commons-logging "1.2"]
                 [commons-io "2.6"]
                 [commons-codec "1.15"]
                 [org.eclipse.jetty/jetty-server "9.4.36.v20210114"]
                 [org.eclipse.jetty/jetty-util "9.4.36.v20210114"]
                 [org.eclipse.jetty/jetty-security "9.4.36.v20210114"]
                 [org.eclipse.jetty/jetty-http "9.4.36.v20210114"]
                 [com.google.protobuf/protobuf-java "3.13.0"]
                 [org.apache.commons/commons-lang3 "3.9"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [com.google.guava/guava "30.1.1-jre"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
