(ns crux.datasource)

(defprotocol Datasource
  (entities [this]))

(defprotocol Entity
  (attr-val [this attr])
  (raw-val [this]))
