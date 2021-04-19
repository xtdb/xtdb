(ns core2.main
  (:gen-class))

(defn -main [& args]
  ((requiring-resolve 'core2.cli/start-node-from-command-line) args))
