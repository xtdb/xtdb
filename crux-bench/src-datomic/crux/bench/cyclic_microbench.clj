(ns crux.bench.cyclic-microbench
  (:require [crux.api :as crux]
            [crux.bench :as bench]
            [datomic.api :as d])
  (:import java.time.Duration))

(def ^:dynamic *needle-count* 4)
(def ^:dynamic *haystack-size* 10000)

(defn haystack [create-docs-fn id-key needles size]
  (concat (create-docs-fn id-key false 0 needles)
          (create-docs-fn id-key true needles size)))

(defn create-triangle-docs [id-key haystack? offset n]
  (apply concat (for [i (map #(- (- 0 %) offset 1) (range n))]
                  (let [c (* i 3)
                        b (inc c)
                        a (inc b)
                        c (str c)
                        b (str b)
                        a (str a)]
                    [{id-key a
                      :a true
                      :ref b}
                     {id-key b
                      :ref c}
                     {id-key c
                      :ref (if haystack? b a)}]))))

(def triangle-q '{:find [?a ?b ?c]
                  :where [[?a :a true]
                          [?a :ref ?b]
                          [?b :ref ?c]
                          [?c :ref ?a]]})

(defn create-five-clique-docs [id-key haystack? offset n]
  (apply concat (for [i (map #(- (- 0 %) offset 1) (range n))]
                  (let [e (* i 5)
                        d (inc e)
                        c (inc d)
                        b (inc c)
                        a (inc b)
                        e (str e)
                        d (str d)
                        c (str c)
                        b (str b)
                        a (str a)]
                    [{id-key a
                      :a true
                      :ref [b c d e]}
                     {id-key b
                      :ref [c d e]}
                     {id-key c
                      :ref [d e]}
                     {id-key d
                      :ref (if haystack? [c] [e])}
                     {:a false ;; an actual (non-ID) datom is needed to transact
                      id-key e}]))))

(def five-clique-q '{:find [?a ?b ?c ?d ?e]
                     :where [[?a :a true]
                             [?a :ref ?b]
                             [?a :ref ?c]
                             [?a :ref ?d]
                             [?a :ref ?e]
                             [?b :ref ?c]
                             [?b :ref ?d]
                             [?b :ref ?e]
                             [?c :ref ?d]
                             [?c :ref ?e]
                             [?d :ref ?e]]})

(def shapes {"triangle" [3 create-triangle-docs triangle-q]
             "five-clique" [5 create-five-clique-docs five-clique-q]})

(def datomic-tx-size 100)

(def crux-tx-size 1000)

(defn submit-data [node create-docs]
  (bench/run-bench :ingest
    (bench/with-additional-index-metrics node
      (let [last-tx
            (->> (haystack create-docs :crux.db/id *needle-count* *haystack-size*)
                 (partition-all crux-tx-size)
                 (reduce (fn [last-tx batch]
                           (crux/submit-tx node (vec (for [doc batch] [:crux.tx/put doc]))))
                         nil))]
        (crux/await-tx node last-tx (Duration/ofMinutes 20))
        {:success? true}))))

(defn test-shape [db shape-id shape-q-fn]
  (bench/run-bench (keyword shape-id)
    (let [success? (= (count (shape-q-fn))
                      *needle-count*)]
      {:success? success?})))

(defn with-datomic [f]
  (let [uri (str "datomic:mem://bench")]
    (try
      (d/delete-database uri)
      (d/create-database uri)
      (let [conn (d/connect uri)]
        (try
          (f conn)
          (finally
            (d/release conn))))
      (finally
        (d/delete-database uri)
        (d/shutdown false)))))

(defn round-down [n m]
  (* (Math/floor (/ n m)) m))

(defn submit-datomic-data [conn docs-per-shape create-docs]
  (let [schema [{:db/ident :ref
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many}
                {:db/ident :a
                 :db/valueType :db.type/boolean
                 :db/cardinality :db.cardinality/one}]]
    @(d/transact conn schema)
    (->> (haystack create-docs :db/id *needle-count* *haystack-size*)
         (partition-all (round-down datomic-tx-size docs-per-shape))
         (reduce (fn [^long n entities]
                   (let [done? (atom false)]
                     (while (not @done?)
                       (try
                         @(d/transact conn entities)
                         (reset! done? true)))
                     (+ n (count entities))))
                 0))
    {:success? true}))

;; DataScript
#_(let [schema {:ref {:db/valueType :db.type/ref}}
        conn (ds/create-conn schema)]
    (ds/transact! conn ...)
    (ds/q ... @conn))

(comment
  (for [[shape-id [_ create-docs shape-q]] shapes]
    (binding [*haystack-size* 1000
              *needle-count* 7]
      (bench/with-nodes [node (select-keys bench/nodes ["standalone-rocksdb"])]
        (bench/with-bench-ns (keyword (str "cyclic-queries--" shape-id))
          (bench/with-crux-dimensions
            (submit-data node create-docs)
            (bench/compact-node node)
            (with-open [db (crux/open-db node)]
              (let [shape-q-fn (partial crux/q db shape-q)]
                (test-shape db (str shape-id "-1") shape-q-fn)
                (test-shape db (str shape-id "-2") shape-q-fn)
                (test-shape db (str shape-id "-3") shape-q-fn))))))))

  (for [[shape-id [docs-per-shape create-docs shape-q]] shapes]
    (binding [*haystack-size* 1000
              *needle-count* 7]
      (with-datomic
        (fn [conn]
          (bench/with-bench-ns (keyword (str "cyclic-queries-datomic--" shape-id))
            (bench/with-crux-dimensions
              (submit-datomic-data conn docs-per-shape create-docs)
              (let [db (d/db conn)
                    shape-q-fn (partial d/query {:query shape-q :timeout 30000 :args [db]})]
                (test-shape db (str shape-id "-1") shape-q-fn)
                (test-shape db (str shape-id "-2") shape-q-fn)
                (test-shape db (str shape-id "-3") shape-q-fn)))))))))
