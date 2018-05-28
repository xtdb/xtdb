(ns crux.bench
  (:require [crux.codecs :as c]
            [crux.core :refer [db]]
            [crux.doc :as doc]
            [crux.fixtures :as f :refer [*kv* random-person]]
            [crux.kv :as cr]
            [crux.kv-store :as ks]
            [crux.query :as q])
  (:import java.util.Date))

(defn bench [& {:keys [n batch-size ts query queries kv index] :or {n 1000
                                                                    batch-size 10
                                                                    queries 100
                                                                    query :name
                                                                    ts (Date.)
                                                                    kv :rocks
                                                                    index :kv}}]
  ((case kv
     :rocks f/with-rocksdb
     :lmdb f/with-lmdb
     :mem f/with-memdb)
   (fn []
     (f/with-kv-store
       (fn []
         ;; Insert data
         (time
          (doseq [[i people] (map-indexed vector (partition-all batch-size (take n (repeatedly random-person))))]
            (case index
              :kv
              (cr/-put *kv* people ts)

              :doc
              (do (doc/store-docs *kv* people)
                  (doc/store-txs *kv*
                                 (vec (for [person people]
                                        [:crux.tx/put
                                         (keyword (:crux.kv/id person))
                                         (str (doc/doc->content-hash person))]))
                                 ts
                                 (inc i))))))

         ;; Basic query
         (time
          (let [q (case query
                    :name '{:find [e]
                            :where [[e :name "Ivan"]]}
                    :multiple-clauses '{:find [e]
                                        :where [[e :name "Ivan"]
                                                [e :last-name "Ivanov"]]}
                    :range '{:find [e]
                             :where [[e :age age]
                                     (> age 20)]})
                db-fn (fn [] (case index
                               :kv
                               (db *kv*)

                               :doc
                               (doc/map->DocDatasource {:kv *kv*
                                                        :transact-time ts
                                                        :business-time ts})))]
            ;; Assert this query is in good working order first:
            (assert (pos? (count (q/q (db-fn) q))))
            (doseq [i (range queries)
                    :let [db (db-fn)]]
              (ks/iterate-with
               *kv*
               (fn [i]
                 (q/q db q)))))))))))

;; Datomic: 100 queries against 1000 dataset = 40-50 millis

;; ~500 mills for 1 million
(defn bench-encode [n]
  (let [d (java.util.Date.)]
    (doseq [_ (range n)]
      (c/encode cr/frame-index-eat {:index :eat :eid (rand-int 1000000) :aid (rand-int 1000000) :ts d}))))

;; ~900 ms for 1 million
;; TODO: add new test here, the value frames have been replaced by nippy.
#_(defn bench-decode [n]
  (let [f (cr/encode cr/frame-value-eat {:type :string :v "asdasd"})]
    (doseq [_ (range n)]
      (crux.codecs/decode cr/frame-value-eat f))))

;; Notes codecs benching:
;; in the current world - m is problematic, as it's a map
;; decode is also likely more expensive, due to enum dispatch and the for loop
