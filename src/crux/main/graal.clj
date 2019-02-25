(ns crux.main.graal
  (:require [crux.bootstrap.cli :as cli])
  (:gen-class))

(defn -main [& args]
  (cli/start-system-from-command-line args))
