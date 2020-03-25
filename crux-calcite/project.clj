(defproject juxt/crux-calcite "derived-from-git"
  :description "SQL for Crux using Apache Calcite"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [com.google.guava/guava "24.0-jre"]
                 [com.google.code.findbugs/jsr305 "3.0.2"]
                 [org.eclipse.jetty/jetty-http "9.4.22.v20191022"]
                 [org.apache.calcite/calcite-core "1.21.0" :exclusions [org.apache.commons/commons-lang3
                                                                        org.apache.calcite.avatica/avatica-core
                                                                        com.fasterxml.jackson.core/jackson-core
                                                                        com.fasterxml.jackson.core/jackson-databind
                                                                        com.fasterxml.jackson.core/jackson-annotations]]
                 [org.apache.calcite.avatica/avatica-server "1.16.0"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn)
