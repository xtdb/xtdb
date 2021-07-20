(defproject pro.juxt.crux/crux-sql "<inherited>"
  :description "SQL for Crux using Apache Calcite"

  :plugins [[lein-javadoc "0.3.0"]
            [lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core]
                 [pro.juxt.clojars-mirrors.cheshire/cheshire "5.10.0"]
                 [org.apache.calcite/calcite-core "1.22.0" :exclusions [com.google.code.findbugs/jsr305]]
                 [org.apache.calcite.avatica/avatica-server "1.16.0"]

                 ;; dependency conflict resolution:
                 [commons-logging "1.2"]
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
             :test {:dependencies [[pro.juxt.crux/crux-test]]}}

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]

  :javadoc-opts {:package-names ["crux"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "Crux SQL Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"
                                   "-link" "https://javadoc.io/static/org.apache.calcite/calcite-core/1.16.0"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}})
