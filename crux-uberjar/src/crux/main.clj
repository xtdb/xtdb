(ns crux.main
  (:gen-class))

(defn -main [& args]
  (require 'crux.cli)
  ((resolve 'crux.cli/start-node-from-command-line) args))
