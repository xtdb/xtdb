(ns crux.fixtures.api
  (:require [crux.api :as crux])
  (:import [crux.api Crux ICruxAPI]
           [java.util ArrayList List]))

(def ^:dynamic ^ICruxAPI *api*)
(def ^:dynamic *opts* nil)

(defn with-opts [opts f]
  (binding [*opts* (merge *opts* opts)]
    (f)))

(defn with-node [f]
  (with-open [node (Crux/startNode *opts*)]
    (binding [*api* node]
      (f))))

(defn submit+await-tx [tx-ops]
  (let [tx (crux/submit-tx *api* tx-ops)]
    (crux/await-tx *api* tx)
    tx))

;; Literal vectors aren't type hinted as List in Clojure, and cannot
;; be type hinted without via a var.
(defn vec->array-list ^java.util.List [^List v]
  (ArrayList. v))
