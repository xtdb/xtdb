(defproject juxt/crux-sql "crux-git-version-alpha"
  :description "SQL for Crux using Apache Calcite"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [cheshire "5.10.0"]
                 [org.apache.calcite/calcite-core "1.22.0"]
                 [org.apache.calcite.avatica/avatica-server "1.16.0"]

                 ;; dependency conflict resolution:
                 [commons-logging "1.2"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[juxt/crux-test "crux-git-version"]

                                   ;; dependency conflict resolution:
                                   [com.google.guava/guava "26.0-jre"]
                                   [com.google.code.findbugs/jsr305 "3.0.2"]
                                   [org.apache.commons/commons-lang3 "3.9"]
                                   [commons-io "2.6"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :jvm-opts ["-Dlogback.configurationFile=resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn)
