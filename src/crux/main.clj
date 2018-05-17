(ns crux.main
  (:gen-class))

(defn -main [& args]
  (require 'crux.system)
  ((resolve 'crux.system/start-system) args))
