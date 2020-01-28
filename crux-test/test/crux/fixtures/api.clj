(ns crux.fixtures.api
  (:require [crux.api :as crux])
  (:import [crux.api Crux ICruxAPI]
           [java.util ArrayList List]))

(def ^:dynamic ^ICruxAPI *api*)
(def ^:dynamic *node*)
(def ^:dynamic *opts* nil)

(defn with-opts [opts f]
  (binding [*opts* (merge *opts* opts)]
    (f)))

(defn with-node [f]
  (with-open [api (Crux/startNode *opts*)]
    (binding [*api* api
              *node* (:node api)]
      (f))))

(defn submit+await-tx [tx-ops]
  (let [tx (crux/submit-tx *node* tx-ops)]
    (crux/await-tx *node* tx)
    tx))

;; Literal vectors aren't type hinted as List in Clojure, and cannot
;; be type hinted without via a var.
(defn vec->array-list ^java.util.List [^List v]
  (ArrayList. v))
