(ns xtdb.main
  (:gen-class))

(defn -main [& args]
  (println (str "Starting XTDB 2.x"
                (when-let [version (System/getenv "XTDB_VERSION")]
                  (str " @ " version))

                (when-let [git-sha (System/getenv "GIT_SHA")]
                  (str " @ "
                       (-> git-sha
                           (subs 0 7))))
                " ..."))
  ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))
