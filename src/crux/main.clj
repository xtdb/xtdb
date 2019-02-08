(ns crux.main
  (:gen-class))

(defn -main [& args]
  ((requiring-resolve 'crux.bootstrap.cli/start-system-from-command-line) args))
