(defproject juxt/console-demo "crux-git-version"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.crux/crux-rocksdb "crux-git-version"]
                 [pro.juxt.crux/crux-http-server "crux-git-version"]
                 [pro.juxt.crux/crux-metrics "crux-git-version"]
                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]]

  :middleware [leiningen.project-version/middleware]
  :uberjar-name "crux-console-demo.jar"
  :aot [crux.console-demo.main]
  :main crux.console-demo.main
  :pedantic? :warn)
