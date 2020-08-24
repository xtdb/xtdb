(defproject juxt/console-demo "crux-git-version"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-rocksdb "crux-git-version-beta"]
                 [juxt/crux-http-server "crux-git-version-alpha"]
                 [juxt/crux-metrics "crux-git-version-alpha"]
                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]]

  :middleware [leiningen.project-version/middleware]
  :uberjar-name "crux-console-demo.jar"
  :aot [crux.console-demo.main]
  :main crux.console-demo.main
  :pedantic? :warn)
