(ns juxt.edge.server.pretty-repl
  (:require
    [fipp.edn :as fipp]
    [clojure.core.server :as clojure.server]
    [clojure.main :as m]))

(defn repl
  []
  (m/repl
    :init clojure.server/repl-init
    :read clojure.server/repl-read
    :print fipp/pprint))

(defn -main
  [& args]
  (repl))
