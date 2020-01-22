(ns crux.fixtures.doc-store
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.fixtures.api :as apif]
            [crux.io :as cio])
  (:import java.io.Closeable))

(defrecord InMemDocumentStore [docs]
  Closeable
  (close [_])

  db/RemoteDocumentStore
  (fetch-docs [this ids]
    (log/debug "Fetching" (cio/pr-edn-str ids))
    (into {}
          (for [id ids]
            [id (get @docs id)])))

  (submit-docs [this id-and-docs]
    (doseq [[content-hash doc] id-and-docs]
      (log/debug "Storing" (cio/pr-edn-str content-hash))
      (swap! docs assoc content-hash doc))))

(def document-store
  {:start-fn (fn [_ _] (->InMemDocumentStore (atom {})))})

(defn with-remote-doc-store-opts [f]
  (apif/with-opts {:crux.node/document-store 'crux.fixtures.doc-store/document-store
                   :crux.kafka/doc-indexing-consumer 'crux.kafka/doc-indexing-from-tx-topic-consumer} f))
