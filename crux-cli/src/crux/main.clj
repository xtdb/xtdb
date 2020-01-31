(ns crux.main
  (:gen-class))

(defn -main [& args]
  ((requiring-resolve 'crux.cli/start-node-from-command-line) args))
