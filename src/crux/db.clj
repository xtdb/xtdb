(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a v])
  (attr-val [this eid attr]))
