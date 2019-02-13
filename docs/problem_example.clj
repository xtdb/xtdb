(ns problem-example
  (:require [crux.bootstrap.standalone :as standalone]
            [clojure.java.io :as io]
            [crux.io :as cio]
            [crux.db :as db]
            [crux.tx :as tx]
            [crux.query :as q])
  (:import [java.util UUID]))

;; This example was reported as a problem, see the last query not
;; returning as expected.

(def system (standalone/start-standalone-system {:kv-backend "crux.kv.rocksdb.RocksKv"
                                                 :db-dir "data"}))

(defn kv [] (:kv-store system))

;; PUT picasso
(db/submit-tx
 (tx/->KvTxLog (kv))
 [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
   {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
    :name "Pablo"
    :last-name "Picasso"}
   #inst "2018-05-18T09:21:27.966-00:00"]])

;; PUT picasso again at a later valid time with a different name
(def tx
  (db/submit-tx
   (tx/->KvTxLog (kv))
   [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
     {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
      :name "Pablo"
      :last-name "Picasso the great"}
     #inst "2018-06-18T09:21:27.966-00:00"]]))

;; Didn't exist yet
(q/q (q/db (kv) #inst "2018-05")
     '{:find [last-name]
       :where [[e :name "Pablo"]
               [e :last-name last-name]]})
;;=> #{}

;; first version
(q/q (q/db (kv) #inst "2018-06")
     '{:find [last-name]
       :where [[e :name "Pablo"]
               [e :last-name last-name]]})
;;=> #{["Picasso"]}

;; second version
(q/q (q/db (kv) #inst "2018-07")
     '{:find [last-name]
       :where [[e :name "Pablo"]
               [e :last-name last-name]]})
;;=> #{["Picasso the great"]}

;; no valid time i.e. "now"
;; expected: newest version, but got first version
(q/q (q/db (kv))
     '{:find [last-name]
       :where [[e :name "Pablo"]
               [e :last-name last-name]]})
;;=> #{["Picasso"]}
