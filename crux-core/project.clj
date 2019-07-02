(defproject juxt/crux-core :derived-from-git
  :description "Crux Core"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.agrona/agrona "1.0.0"]]
  :profiles {:dev {:dependencies [[juxt/crux-dev :derived-from-git]]}}
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
