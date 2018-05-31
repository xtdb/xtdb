(ns crux.bench
  (:require [criterium.core :as crit]
            [crux.codecs :as c]
            [crux.core :refer [db]]
            [crux.doc :as doc]
            [crux.fixtures :as f :refer [*kv* random-person]]
            [crux.kv :as cr]
            [crux.kv-store :as ks]
            [crux.query :as q])
  (:import java.util.Date))

(def bench-queries {
              :name '{:find [e]
                      :where [[e :name "Ivan"]]}
              :multiple-clauses '{:find [e]
                                  :where [[e :name "Ivan"]
                                          [e :last-name "Ivanov"]]}
              :join '{:find [e2]
                      :where [[e :last-name "Ivanov"]
                              [e :last-name name1]
                              [e2 :last-name name1]]}
              :range '{:find [e]
                       :where [[e :age age]
                               (> age 20)]}})

(defn- insert-data [n batch-size ts index]
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

(defn- perform-query [ts query index]
  (let [q (query bench-queries)
        db-fn (fn [] (case index
                       :kv
                       (db *kv*)

                       :doc
                       (doc/map->DocDatasource {:kv *kv*
                                                :transact-time ts
                                                :business-time ts})))]
    ;; Assert this query is in good working order first:
    (assert (pos? (count (q/q (db-fn) q))))
    
    (let [db (db-fn)]
      (ks/iterate-with
       *kv*
       (fn [_]
         (q/q db q))))))

(defn crit-bench
  "Currently: prints the insert-data time, and returns the average time taken by a single query (in msecs)"
  [& {:keys [n batch-size ts query queries kv index quick]
      :or {n 1000
           batch-size 10
           queries 100
           query :name
           ts (Date.)
           kv :rocks
           index :kv
           quick true}}]
  ((case kv
     :rocks f/with-rocksdb
     :lmdb f/with-lmdb
     :mem f/with-memdb)
   (fn []
     (f/with-kv-store
       (fn []

         {:insert-time
          (-> (with-out-str (time
                             (insert-data n batch-size ts index)))
              (clojure.string/split #" ")
              (nth 2)
              read-string)

          :query-time
          (->
           (if quick
             (crit/quick-benchmark
              (perform-query ts query index) {:samples queries})
             (crit/benchmark
              (perform-query ts query index) {:samples queries}))
           :mean
           first
           (* 1000) ;; secs -> msecs
           )})))))

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
                    :join '{:find [e2]
                            :where [[e :last-name "Ivanov"]
                                    [e :last-name name1]
                                    [e2 :last-name name1]]}
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
