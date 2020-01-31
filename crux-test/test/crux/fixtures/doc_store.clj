(ns crux.fixtures.doc-store
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.fixtures.api :as apif]
            [crux.index :as i]
            [crux.io :as cio]
            [crux.topology :as t]
            [crux.object-store :as os])
  (:import java.io.Closeable))

(defrecord InMemDocumentStore [docs]
  Closeable
  (close [_])

  db/DocumentStore
  (fetch-docs [this ids]
    (log/debug "Fetching" (cio/pr-edn-str ids))
    (into {}
          (for [id ids]
            [id (get @docs id)])))

  (submit-docs [this id-and-docs]
    (log/debug "Storing" id-and-docs)
    (doseq [[content-hash doc] id-and-docs]
      (swap! docs assoc content-hash doc)))

  db/ObjectStore
  (get-single-object [this _ k]
    (os/keep-non-evicted-doc (get (db/fetch-docs this [k]) k)))
  (get-objects [this _ ks]
    (into {}
          (for [[k doc] (db/fetch-docs this ks)
                :when (os/keep-non-evicted-doc doc)]
            [k doc])))
  (known-keys? [this _ ks]
    (every? @docs ks))
  (put-objects [this kvs]
    (db/submit-docs this kvs)))

(def document-store
  {:start-fn (fn [_ _] (->InMemDocumentStore (atom {})))})

(defn with-remote-doc-store-opts [f]
  (apif/with-opts {:crux.node/document-store 'crux.fixtures.doc-store/document-store
                   :crux.kafka/doc-indexing-consumer 'crux.kafka/doc-indexing-from-tx-topic-consumer}
    f))

(defn with-same-doc-object-store [f]
  (apif/with-opts {:crux.node/object-store (t/refer-module :crux.node/document-store)} f))
