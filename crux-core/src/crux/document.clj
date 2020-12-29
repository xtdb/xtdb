(ns ^:no-doc crux.document
  (:import (crux.api IDocument)
           (java.io Writer)))

(defrecord Document []
  IDocument
  (getDocumentId [this] (:crux.db/id this))
  (getContents [this] (->> :crux.db/id
                           (dissoc this)
                           (into {}))))

(defmethod print-method Document [document ^Writer w]
  (.write w "#crux/document ")
  (print-method (into {} document) w))

(defn ?->Document [d]
  (if (and
        (map? d)
        (contains? d :crux.db/id)
        (not (and
               (contains? d :crux.db/evicted?)
               (:crux.db/evicted? d))))
    (->Document d)
    d))

(defn ->Document [d]
  (map->Document d))