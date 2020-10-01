(ns crux.fixtures.lucene
  (:require [crux.fixtures :as fix :refer [with-tmp-dir]]
            [crux.lucene :as l]))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts {::l/node {:db-dir (.toPath ^java.io.File db-dir)}
                    :crux/indexer {:crux/module 'crux.lucene/->indexer
                                   :indexer 'crux.kv.indexer/->kv-indexer}}
      f)))
