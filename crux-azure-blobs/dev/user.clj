(ns user
  (:require [clojure.java.io :as io]
            [crux.api :as crux]
            [crux.azure.blobs :as b]))

(defn start-node [storage-dir]
  (crux/start-node {:crux.node/topology '[crux.standalone/topology
                                          crux.azure.blobs/doc-store]
	            :crux.standalone/event-log-dir (io/file storage-dir "event-log")
	            :crux.kv/db-dir (io/file storage-dir "indexes")
                    ::b/sas-token (System/getenv "CRUX_AZURE_BLOBS_SAS_TOKEN")
                    ::b/storage-account (System/getenv "CRUX_AZURE_BLOBS_STORAGE_ACCOUNT")
                    ::b/container (System/getenv "CRUX_AZURE_BLOBS_CONTAINER")}))

(defn await-ingest
  [node docs]
  (crux/await-tx node
                 (crux/submit-tx node
                                 (vec (for [doc docs]
                                        [:crux.tx/put doc])))))

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

   {:crux.db/id :team/some-team
    :team/name "Some Team"
    :experience-level 10
    :org :org/some-org
    :requirements #{:course/math101}}])

(defn ingest-query-entity []
  (with-open [node (start-node "/tmp/")]
    (await-ingest node init-data)
    (let [db (crux/db node)]
      (->> {:find '[cls]
            :where '[[cls :requirements cid]]
            :args '[{cid :course/math101}]}
           (crux/q db)
           (map first)
           (map #(crux/entity db %))))))
