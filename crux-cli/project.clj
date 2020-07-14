(defproject juxt/crux-cli "crux-git-version-beta"
  :description "Crux Uberjar"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "0.4.2"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-http-server "crux-git-version-alpha"]
                 [juxt/crux-test "crux-git-version" :scope "test"]

                 ;; Dependency resolution
                 [org.ow2.asm/asm "7.1" :scope "test"]]

  :middleware [leiningen.project-version/middleware]
  :aot [crux.main]
  :main crux.main
  :uberjar-name "crux-standalone.jar"
  :pedantic? :warn)
