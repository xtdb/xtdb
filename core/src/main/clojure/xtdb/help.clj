(ns ^:no-doc xtdb.help)

(defn print-help []
  (println "--- XTDB ---")
  (newline)
  (println "XTDB has several top-level commands to choose from:")
  (println " * `node` (default, can be omitted): starts an XT node")
  (println " * `compactor`: runs a compactor-only node")
  (println " * `playground`: starts a 'playground', an in-memory node which accepts any database name, creating it if required")
  (println " * `reset-compactor <db-name>`: resets the compacted files on the given node.")
  (newline)
  (println "For more information about any command, run `<command> --help`, e.g. `playground --help`"))