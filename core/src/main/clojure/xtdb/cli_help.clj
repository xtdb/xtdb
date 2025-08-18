(ns ^:no-doc xtdb.cli-help
  (:require [clojure.string :as str]))

(defn version-string []
  (let [version (some-> (System/getenv "XTDB_VERSION")
                        str/trim
                        not-empty)
        git-sha (some-> (System/getenv "GIT_SHA")
                        str/trim
                        not-empty
                        (subs 0 7))]
    (str "XTDB"
         (if version (str " " version) " 2.x")
         (when git-sha (str " [" git-sha "]")))))

(defn print-help []
  (println (str "--- " (version-string) " ---"))
  (newline)
  (println "XTDB has several top-level commands to choose from:")
  (println " * `node` (default, can be omitted): starts an XT node")
  (println " * `compactor`: runs a compactor-only node")
  (println " * `playground`: starts a 'playground', an in-memory node which accepts any database name, creating it if required")
  (println " * `reset-compactor <db-name>`: resets the compacted files on the given node.")
  (newline)
  (println "For more information about any command, run `<command> --help`, e.g. `playground --help`"))