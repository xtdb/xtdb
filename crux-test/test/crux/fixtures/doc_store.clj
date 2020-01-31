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
      (swap! docs assoc content-hash doc))))

(defrecord DocumentStoreBackedObjectStore [document-store]
  Closeable
  (close [_])

  db/ObjectStore
  (get-single-object [this _ k]
    (os/keep-non-evicted-doc (get (db/fetch-docs document-store [k]) k)))
  (get-objects [this _ ks]
    (into {}
          (for [[k doc] (db/fetch-docs document-store ks)
                :when (os/keep-non-evicted-doc doc)]
            [k doc])))
  (known-keys? [this _ ks]
    (let [docs (db/fetch-docs document-store ks)]
      (every? docs ks)))
  (put-objects [this kvs]
    (db/submit-docs document-store kvs)))

(def document-store
  {:start-fn (fn [_ _] (->InMemDocumentStore (atom {})))})

(def object-store
  {:start-fn (fn [{:keys [crux.node/document-store]} _]
               (->DocumentStoreBackedObjectStore document-store))
   :deps [:crux.node/document-store]})

(defn with-remote-doc-store-opts [f]
  (apif/with-opts {:crux.node/document-store 'crux.fixtures.doc-store/document-store
                   :crux.kafka/doc-indexing-consumer 'crux.kafka/doc-indexing-from-tx-topic-consumer}
    f))

(defn with-doc-backed-object-store [f]
  (apif/with-opts {:crux.node/object-store 'crux.fixtures.doc-store/object-store} f))
