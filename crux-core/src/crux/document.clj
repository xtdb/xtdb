(ns ^:no-doc crux.document
  (:import (crux.api IDocument)
           (java.io Writer)))

(defrecord Document [d]
  IDocument
  (getDocumentId [this] (:crux.db/id d))
  (getContents [this] (dissoc d :crux.db/id)))

(defmethod print-method Document [d ^Writer w]
  (.write w "#crux/document ")
  (print-method (into {} d) w))

(defn ?->Document [d]
  (if (and (map? d) (contains? d :crux.db/id))
    (->Document d)
    d))

(defn ->Document [d]
  (Document. d))