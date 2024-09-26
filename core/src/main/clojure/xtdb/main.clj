(ns xtdb.main
  (:gen-class)
  (:require [clojure.string :as str]))

(defn -main [& args]
  (println (str "Starting XTDB 2.x"
                (when-let [version (System/getenv "XTDB_VERSION")]
                  (when-not (str/blank? version)
                    (str " @ " version)))

                (when-let [git-sha (System/getenv "GIT_SHA")]
                  (when-not (str/blank? git-sha)
                    (str " @ "
                         (-> git-sha
                             (subs 0 7)))))
                " ..."))
  ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))
