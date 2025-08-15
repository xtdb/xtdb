(ns xtdb.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn -main [& args]
  (let [version (some-> (System/getenv "XTDB_VERSION")
                        str/trim
                        not-empty)
      git-sha (some-> (System/getenv "GIT_SHA")
                      str/trim
                      not-empty
                      (subs 0 7))]
    (log/info (str "Starting XTDB"
                   (if version (str " " version) " 2.x")
                   (when git-sha (str " [" git-sha "]"))
                   " ...")))
  ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))
