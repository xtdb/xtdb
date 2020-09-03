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

   {:crux.db/id :region/some-region
    :country :country/denmark}

   {:crux.db/id :municipality/some-municipality
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

    ;; why is there a separate call to fetch-docs when below timeout is 500ms?
    (Thread/sleep 450)

    ;; This println returns nil:
    (println 'query (q node {:find '[?name] 
                             :where '[[c-id :course/name ?name]]
                             :args [{'c-id (:crux.db/id :course/math101)}]}))
    
    ;; Even though the println in blobs fetch-docs returns the correct doc,
    ;; no doc is returned from the call to crux/entity:
    (println 'entity (crux/entity (crux/db node) :course/math101))

    ;; full console output:
    ;; 
    ;; fetch-docs 6
    ;; get-blob e4a659d48c3fb2bfaa45825ca1b0990bbebe9aea
    ;; get-blob 2bc6c43223b1e505d0ba8cb6577e2f5493698c8a
    ;; get-blob ce724cbba2c9e510d57451337d1e7be24f906dca
    ;; get-blob eefb2e4b30d60e235f48def70545be43a3d12d8a
    ;; get-blob 96baab63fa904862a6f906c452e80ef0b1202ea2
    ;; get-blob ef928950a1dc9ec2459c6f00e508f3c4751a485c
    ;; query #{}
    ;; fetch-docs 0
    ;; entity {:crux.db/id :course/math101, :course/name Math 101, :level :a, :org :org/some-org}
    ))
