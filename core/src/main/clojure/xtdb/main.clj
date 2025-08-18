(ns xtdb.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.cli-help :as cli-help]))

(defn- help-requested? [args]
  (contains? #{"-h" "--help" "help"} (first args)))

(defn -main [& args]
  (if (help-requested? args)
    (do
      (cli-help/print-help)
      (System/exit 0))
    (do
      (log/info (str "Starting " (cli-help/version-string) " ..."))
      ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))))
