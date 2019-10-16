(ns crux.bench
  (:require [criterium.core :as crit]
            [crux.api :as api]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :refer [*api*] :as apif]
            [crux.fixtures.kv :as fkv]
            [crux.query :as q])
  (:import java.util.Date))

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
                               [(> age 20)]]}
              :hardcoded-name '{:find [e]
                                :where [[e :name name]]
                                :args [{:name "davros"}]}})

(defmacro duration
  "Times the execution of a function,
    discarding the output and returning the elapsed time in seconds"
  ([& forms]
   `(let [start# (System/nanoTime)]
      ~@forms
      (double (/ (- (System/nanoTime) start#) 1e9)))))

(defmacro duration-millis
  "Times the execution of a function,
    discarding the output and returning the elapsed time in milliseconds"
  ([& forms]
   `(let [start# (System/nanoTime)]
      ~@forms
      (double (/ (- (System/nanoTime) start#) 1e6)))))

(defn- insert-docs [ts docs]
  (api/submit-tx *api* (f/maps->tx-ops docs ts)))

(defn- insert-data [n batch-size ts]
  (doseq [[i people] (map-indexed vector (partition-all batch-size (take n (repeatedly f/random-person))))]
    (insert-docs ts people)))

(defn- perform-query [ts query]
  (let [q (query queries)
        q-fn (fn []
               (let [db (api/db *api* ts)]
                 (if (= query :id)
                   (api/entity db :hardcoded-id)
                   (q/q db q))))]
    ;; Assert this query is in good working order first:
    (assert (pos? (count (q-fn))))
    (q-fn)))

(defn- do-benchmark [ts samples speed verbose query]
  (when verbose
    (print (str query "... ")) (flush)
    (print (str (perform-query ts query) "... ")) (flush))
  (let [result
        (-> (case speed
              :normal
              (-> (crit/benchmark
                   (perform-query ts query) {:samples samples})
                  :mean
                  first)

              :quick ;; faster but "less rigorous"
              (-> (crit/quick-benchmark
                   (perform-query ts query) {:samples samples})
                  :mean
                  first)

              :instant ;; even faster, even less rigorous
              (as-> (map (fn [_] (duration (perform-query ts query)))
                         (range samples)) x
                (apply + x)
                (/ x samples)))
            (* 1000))] ;; secs -> msecs
    (when verbose (println result))
    result))

(defn bench
  [& {:keys [n batch-size ts query samples kv speed verbose preload]
      :or {n 1000
           batch-size 10
           samples 100 ;; should always be >2
           query :name
           ts (Date.)
           kv :rocks
           speed :instant
           verbose false}}]
  ((case kv
     :rocks fkv/with-rocksdb
     :lmdb fkv/with-lmdb
     :mem fkv/with-memdb)
   (fn []
     (fkv/with-kv-dir
       (fn []
         (fs/with-standalone-node
           (fn []
             (apif/with-node
               (fn []
                 (when verbose (print ":insert... ") (flush))
                 (when preload
                   (insert-docs ts preload))
                 (let [insert-time (duration (insert-data n batch-size ts))
                       queries-to-bench (if (= query :all)
                                          (keys queries)
                                          (flatten [query]))]
                   (when verbose (println insert-time))
                   (merge {:insert insert-time}
                          (zipmap
                           queries-to-bench
                           (map (partial do-benchmark ts samples speed verbose)
                                queries-to-bench)))))))))))))
(comment
  (bench)

  (bench :verbose true :preload [(assoc (f/random-person)
                                        :crux.db/id :hardcoded-id
                                        :name "davros")]
         :n 10000
         :samples 10000
         :query :hardcoded-name) ;;2.5

  (bench :verbose true :preload [(assoc (f/random-person)
                                        :crux.db/id :hardcoded-id
                                        :name "davros")]
         :n 10000
         :samples 10000
         :query :id) ;;2.3
  )
