(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a v]))

(defprotocol Entity
  (attr-val [this attr])
  (raw-val [this]))
