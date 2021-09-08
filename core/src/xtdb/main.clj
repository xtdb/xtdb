(ns xtdb.main
  (:gen-class))

(defn -main [& args]
  ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))
