(ns crux.main
  (:gen-class))

(defn -main [& args]
  (require 'crux.bootstrap)
  ((resolve 'crux.bootstrap/start-system) args))
