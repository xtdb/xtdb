(ns core2.main
  (:gen-class))

(defn -main [& args]
  (println (str "Starting XTDB Core2 (pre-alpha) @ " (subs (System/getenv "GIT_SHA") 0 7) " ..."))
  ((requiring-resolve 'core2.cli/start-node-from-command-line) args))
