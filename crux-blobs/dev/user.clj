(ns user
  (:require [clojure.java.io :as io]
            [crux.api :as crux]))

(defn start-node [storage-dir]
  (crux/start-node {:crux.node/topology '[crux.standalone/topology crux.blobs/blobs-doc-store]
	            :crux.standalone/event-log-dir (io/file storage-dir "event-log")
	            :crux.kv/db-dir (io/file storage-dir "indexes")
                    :crux.blobs/storage-account (System/getenv "CRUX_BLOBS_STORAGE_ACCOUNT")
                    :crux.blobs/container (System/getenv "CRUX_BLOBS_CONTAINER")}))

(defn ingest
  [node docs]
  (crux/submit-tx node
                  (vec (for [doc docs]
                         [:crux.tx/put doc]))))

(defn q [node query]
  (crux/q (crux/db node) query))

(def init-data
  [{:crux.db/id :country/denmark
    :country/name "Denmark"}

   {:crux.db/id :region/hovedstaden
    :country :country/denmark}

   {:crux.db/id :municipality/copenhagen
    :region :region/hovedstaden}

   {:crux.db/id :org/some-org
    :org/name "Some org name"
    :municipality :municipality/copenhagen}

   {:crux.db/id :course/math101
    :course/name "Math 101"
    :level :a
    :org :org/some-org}

   {:crux.db/id :class/some-class
    :class/name "Some class"
    :level :a
    :org :org/some-org
    :courses [:course/math101]}])

(defn n []
  (with-open [node (start-node "/tmp/")]
    (ingest node init-data)

    ;; workaround - need to synchronize the ingestion above!
    (Thread/sleep 2500)
    (println 'query (q node {:find '[?name] 
                             :where '[[c-id :course/name ?name]]
                             :args [{'c-id :course/math101}]}))
    
    (println 'entity (crux/entity (crux/db node) :country/denmark))))
