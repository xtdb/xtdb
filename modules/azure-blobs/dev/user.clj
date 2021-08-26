(ns user
  (:require [crux.api :as xt]
            [xtdb.azure.blobs :as azb]))

(def init-data
  [{:xt/id :country/denmark
    :country/name "Denmark"}

   {:xt/id :region/hovedstaden
    :country :country/denmark}

   {:xt/id :municipality/copenhagen
    :region :region/hovedstaden}

   {:xt/id :org/some-org
    :org/name "Some org name"
    :municipality :municipality/copenhagen}

   {:xt/id :course/math101
    :course/name "Math 101"
    :level :a
    :org :org/some-org}

   {:xt/id :team/some-team
    :team/name "Some Team"
    :experience-level 10
    :org :org/some-org
    :requirements #{:course/math101}}])

(defn start-node []
  (xt/start-node
   {:xt/document-store {:xt/module `azb/->document-store
                        :sas-token (System/getenv "XTDB_AZURE_BLOBS_SAS_TOKEN")
                        :storage-account (System/getenv "XTDB_AZURE_BLOBS_STORAGE_ACCOUNT")
                        :container (System/getenv "XTDB_AZURE_BLOBS_CONTAINER")}}))

(defn await-ingest
  [node docs]
  (xt/await-tx node
                 (xt/submit-tx node
                                 (vec (for [doc docs]
                                        [:xt/put doc])))))

(defn ingest-query-entity []
  (with-open [node (start-node)]
    (await-ingest node init-data)
    (let [db (xt/db node)]
      (->> {:find '[cls]
            :where '[[cls :requirements cid]]
            :args '[{cid :course/math101}]}
           (xt/q db)
           (map first)
           (map #(xt/entity db %))))))
