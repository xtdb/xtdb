(defproject pro.juxt.crux/crux-core "crux-git-version-beta"
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.slf4j/slf4j-api "1.7.30"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [pro.juxt.clojars-mirrors.com.stuartsierra/dependency "1.0.0"]
                 [pro.juxt.clojars-mirrors.com.taoensso/nippy "3.1.1"]
                 [org.clojure/tools.reader "1.3.5"]
                 [org.clojure/data.json "2.3.1"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.agrona/agrona "1.11.0"]
                 [com.github.jnr/jnr-ffi "2.2.4" :scope "provided"]
                 [pro.juxt.clojars-mirrors.edn-query-language/eql "2021.02.28"]]

  :plugins [[lein-javadoc "0.3.0"]]

  :profiles {:dev {:jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]
                   :dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :cli-e2e-test {:jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]
                            :dependencies [[pro.juxt.crux/crux-http-server "crux-git-version-alpha"]]}
             :test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]
  :aot [crux.main]

  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :pedantic? :warn

  :javadoc-opts {:package-names ["crux.api"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "Crux Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}})
