(defproject juxt/console-demo "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-http-server "derived-from-git"]
                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]]

  :middleware [leiningen.project-version/middleware]
  :uberjar-name "crux-console-demo.jar"
  :aot [crux.console-demo.main]
  :main crux.console-demo.main
  :pedantic? :warn)
