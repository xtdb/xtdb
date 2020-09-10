(ns user
  (:require [clojure.java.io :as io]
            [crux.api :as crux]
            [crux.rocksdb :as rocks]
            [crux.azure.blobs :as azb]))

(defn start-node [storage-dir]
  (crux/start-node {:crux/tx-log {:crux/module `rocks/->kv-store
                                  :db-dir (io/file storage-dir "tx-log")}
                    :crux/document-store {:crux/module `azb/->document-store
                                          :sas-token (System/getenv "CRUX_AZURE_BLOBS_SAS_TOKEN")
                                          :storage-account (System/getenv "CRUX_AZURE_BLOBS_STORAGE_ACCOUNT")
                                          :container (System/getenv "CRUX_AZURE_BLOBS_CONTAINER")}
                    :crux/indexer {:crux/module `rocks/->kv-store
                                   :db-dir (io/file storage-dir "indexes")}}))

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
