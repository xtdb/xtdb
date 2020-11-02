(ns crux.fixtures.lucene
  (:require [crux.fixtures :as fix :refer [with-tmp-dir]]
            [crux.lucene :as l]))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts {::l/lucene-store {:db-dir db-dir}}
      f)))
