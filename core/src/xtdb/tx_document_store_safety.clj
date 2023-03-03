(ns xtdb.tx-document-store-safety
  "During transaction processing it is important that document store fetches only fail deterministically.

  This namespace provides a version of fetch that allows you to panic on any exception without blowing up for simple failures like
  temporary unavailability of the document store."
  (:require [clojure.set :as set]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.io :as xio]))

(def ^:dynamic *in-tx* false)
(def ^:dynamic *exp-backoff-opts* nil)

(defn fetch-docs
  "A version of fetch docs that when in a transaction (*in-tx* is true) implements additional safety semantics
  that are important for the integrity of transaction processing across multiple nodes.

  - Exceptions thrown by the doc store are retried according to the options of xio/exp-backoff, avoiding an ingester panic for errors relating to unavailable.
  - Missing documents cause an IllegalStateException to be propagated to the caller (without backoff), this will cause an ingester panic.

  Options to the xio/exp-backoff loop can be varied with *exp-backoff-opts*.

  If not in a transaction, identical to db/fetch-docs."
  [document-store ids]
  (if *in-tx*
    (let [doc-hashes (into #{} (map c/new-id) ids)
          docs (xio/exp-backoff #(db/fetch-docs document-store doc-hashes) *exp-backoff-opts*)
          fetched-doc-hashes (set (keys docs))]
      (when-not (= fetched-doc-hashes doc-hashes)
        (let [missing-docs (set/difference doc-hashes fetched-doc-hashes)
              ex-data {:cognitect.anomalies/category :cognitect.anomalies/not-found
                       :missing-docs missing-docs}]
          (throw (xtdb.IllegalStateException. (str "missing docs: " (pr-str missing-docs)) ex-data nil))))
      docs)
    (db/fetch-docs document-store ids)))
