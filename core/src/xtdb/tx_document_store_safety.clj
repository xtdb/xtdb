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
  "Like db/fetch-docs, but exceptions thrown by the doc store cause retries according to the options of xio/exp-backoff, avoiding an ingester panic for errors relating to unavailability.

  Options to the xio/exp-backoff loop can be varied with *exp-backoff-opts*.

  If not in a transaction, identical to db/fetch-docs."
  [document-store ids]
  (if *in-tx*
    (xio/exp-backoff #(db/fetch-docs document-store ids) *exp-backoff-opts*)
    (db/fetch-docs document-store ids)))

(defn fetch-docs-throw-on-missing
  "Like fetch-docs, but missing documents cause an IllegalStateException to be propagated to the caller (without backoff), in a transaction - this will cause an ingester panic.
  This is important during transaction processing to avoid consistency issues if documents were present at one point in time but are no longer available (due to say, corruption of the doc store, accidental deletion or a backup restore)

  CAVEAT: currently missing entries are valid for pull/entity calls during transaction functions, so this only applies for fetching referenced docs, tx fn args etc.

  Always returns xtdb.codec/Id instances as the id keys, regardless of the type of id that was supplied."
  [document-store ids]
  (let [doc-hashes (into #{} (map c/new-id) ids)
        docs (fetch-docs document-store ids)
        fetched-doc-hashes (set (keys docs))]
    (when-not (= fetched-doc-hashes doc-hashes)
      (let [missing-docs (set/difference doc-hashes fetched-doc-hashes)
            ex-data {:cognitect.anomalies/category :cognitect.anomalies/not-found
                     :missing-docs missing-docs}]
        (throw (xtdb.IllegalStateException. (str "missing docs: " (pr-str missing-docs)) ex-data nil))))
    docs))
