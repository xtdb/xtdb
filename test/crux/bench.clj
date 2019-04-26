(ns crux.bench
  (:require [criterium.core :as crit]
            [crux.db :as db]
            [crux.tx :as tx]
            [crux.query :as q]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.lru :as lru]
            [crux.bootstrap :as b]
            [crux.index :as idx])
  (:import [java.util Date]))

(def queries {:name '{:find [e]
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
                               [(> age 20)]]}})

(defmacro duration
  "Times the execution of a function,
    discarding the output and returning the elapsed time in seconds"
  ([& forms]
   `(let [start# (System/nanoTime)]
      ~@forms
      (double (/ (- (System/nanoTime) start#) 1e9)))))

(defn- insert-data [n batch-size ts index]
  (doseq [[i people] (map-indexed vector (partition-all batch-size (take n (repeatedly f/random-person))))]
    @(db/submit-tx (tx/->KvTxLog *kv* (idx/->KvObjectStore *kv*)) ; todo do we want LRU caching here?
                   (f/maps->tx-ops people ts))))

(defn- perform-query [ts query index]
  (let [q (query queries)
        db-fn (fn [] (q/db *kv* ts))]
    ;; Assert this query is in good working order first:
    (assert (pos? (count (q/q (db-fn) q))))

    (q/q (db-fn) q)))

(defn- do-benchmark [ts samples index speed verbose query]
  (when verbose (print (str query "... ")) (flush))
  (let [result
        (-> (case speed
              :normal
              (-> (crit/benchmark
                   (perform-query ts query index) {:samples samples})
                  :mean
                  first)

              :quick ;; faster but "less rigorous"
              (-> (crit/quick-benchmark
                   (perform-query ts query index) {:samples samples})
                  :mean
                  first)

              :instant ;; even faster, even less rigorous
              (as-> (map (fn [_] (duration (perform-query ts query index)))
                         (range samples)) x
                (apply + x)
                (/ x samples)))
            (* 1000))] ;; secs -> msecs
    (when verbose (println result))
    result))

(defn bench
  [& {:keys [n batch-size ts query samples kv index speed verbose]
      :or {n 1000
           batch-size 10
           samples 100 ;; should always be >2
           query :name
           ts (Date.)
           kv :rocks
           speed :instant
           verbose false}}]
  ((case kv
     :rocks f/with-rocksdb
     :lmdb f/with-lmdb
     :mem f/with-memdb)
   (fn []
     (f/with-kv-store
       (fn []
         (when verbose (print ":insert... ") (flush))
         (let [insert-time (duration (insert-data n batch-size ts index))
               queries-to-bench (if (= query :all)
                                  (keys queries)
                                  (flatten [query]))]
           (when verbose (println insert-time))
           (merge {:insert insert-time}
                  (zipmap
                   queries-to-bench
                   (map (partial do-benchmark ts samples index speed verbose)
                        queries-to-bench)))))))))
