(ns xtdb.pgwire.playground
  (:require [xtdb.pgwire :as pgwire]))

(defn open-playground
  ([] (open-playground {}))

  ([opts]
   (pgwire/serve nil opts)))

(comment
  (def foo-playground (open-playground)))
