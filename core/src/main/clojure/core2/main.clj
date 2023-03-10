(ns core2.main
  (:gen-class))

(defn -main [& args]
  (println (str "Starting XTDB Core2 (pre-alpha)"
                (when-let [version (System/getenv "CORE2_VERSION")]
                  (str " @ " version))

                (when-let [git-sha (System/getenv "GIT_SHA")]
                  (str " @ "
                       (-> git-sha
                           (subs 0 7))))
                " ..."))
  ((requiring-resolve 'core2.cli/start-node-from-command-line) args))
