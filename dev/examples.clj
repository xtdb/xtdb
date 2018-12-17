(ns examples
  ;; tag::require[]
  (:require [crux.api :as api]
            [crux.db :as db]
            [crux.query :as q])
  ;; end::require[]
  )

;; tag::start-system[]
(def system (api/new-standalone-system {:kv-backend "crux.kv.memdb.MemKv"}))
;; end::start-system[]

;; tag::submit-tx[]
(db/submit-tx
 (:tx-log system)
 [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
   {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
    :name "Pablo"
    :last-name "Picasso"}
   #inst "2018-05-18T09:20:27.966-00:00"]])
;; end::submit-tx[]

;; tag::query[]
(q/q (q/db (:kv-store system))
     '{:find [e]
       :where [[e :name "Pablo"]]})
;; end::query[]

(comment
  ;; tag::should-get[]
  #{[:http://dbpedia.org/resource/Pablo_Picasso]}
  ;; end::should-get[]
  )

(.close system)
