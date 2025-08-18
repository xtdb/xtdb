(ns xtdb.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.help :as help]))

(defn- help-requested? [args]
  (contains? #{"-h" "--help" "help"} (first args)))

(defn -main [& args]
  (if (help-requested? args)
    (do
      (help/print-help)
      (System/exit 0))
    (do
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
      ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))))
