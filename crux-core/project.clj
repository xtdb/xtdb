(defproject juxt/crux-core "crux-git-version-beta"
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.0.0"]
                 [org.slf4j/slf4j-api "1.7.29"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.clojure/tools.reader "1.3.2"]
                 [com.taoensso/encore "2.114.0"]
                 [org.agrona/agrona "1.0.7"]
                 [com.github.jnr/jnr-ffi "2.1.9" :scope "provided"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[juxt/crux-test "crux-git-version" :scope "test"]
                                   ;; Dependency Resolution
                                   [com.github.jnr/jnr-ffi "2.1.12"]
                                   [org.clojure/test.check "0.10.0"]
                                   [org.ow2.asm/asm "7.1"]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]
  :pedantic? :warn)
