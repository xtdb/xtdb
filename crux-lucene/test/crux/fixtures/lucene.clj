(ns crux.fixtures.lucene
  (:require [crux.fixtures :as fix :refer [with-tmp-dir]]
            [crux.lucene :as l]))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts {::l/node {:db-dir (.toPath ^java.io.File db-dir)}
                    :crux/index-store {:crux/module 'crux.lucene/->index-store
                                       :index-store 'crux.kv.index-store/->kv-index-store}}
      f)))
