(ns crux.index-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.tx :as tx])
  (:import java.util.Date))

(t/use-fixtures :each fkv/with-each-kv-store-implementation fkv/with-kv-store f/with-silent-test-check)

;; NOTE: These tests does not go via the TxLog, but writes its own
;; transactions direct into the KV store so it can generate random
;; histories of both valid and transaction time.

(defn gen-date [start-date end-date]
  (gen/fmap #(Date. (long %)) (gen/choose (inst-ms start-date) (inst-ms end-date))))

(defn gen-vt+tt+deleted? [start-date end-date]
  (gen/tuple
   (gen-date start-date end-date)
   (gen-date start-date end-date)
   (gen/frequency [[8 (gen/return false)]
                   [2 (gen/return true)]])))

(defn gen-query-vt+tt [start-date end-date]
  (gen/tuple
   (gen-date start-date end-date)
   (gen-date start-date end-date)))

(defn vt+tt+deleted?->vt+tt->entities [eid txs]
  (let [entities (vec (for [[tx-id [vt tt deleted?]] (map-indexed vector (sort-by second txs))
                            :let [content-hash (c/new-id (keyword (str tx-id)))]]
                        {:crux.db/id (str (c/new-id eid))
                         :crux.db/content-hash (if deleted?
                                                 (str (c/new-id nil))
                                                 (str content-hash))
                         :crux.db/valid-time vt
                         :crux.tx/tx-time tt
                         :crux.tx/tx-id tx-id}))]
    (into (sorted-map)
          (zipmap
           (for [entity-tx entities]
             [(c/date->reverse-time-ms (:crux.db/valid-time entity-tx))
              (c/date->reverse-time-ms (:crux.tx/tx-time entity-tx))])
           entities))))

(defn- write-vt+tt->entities-direct-to-index [kv vt+tt->entity]
  (doseq [[_ entity-tx] vt+tt->entity
          :let [eid (c/->id-buffer (:crux.db/id entity-tx))
                vt (:crux.db/valid-time entity-tx)
                tt (:crux.tx/tx-time entity-tx)
                tx-id (:crux.tx/tx-id entity-tx)
                z (c/encode-entity-tx-z-number vt tt)
                content-hash (c/new-id (:crux.db/content-hash entity-tx))]]
    (kv/store kv [[(c/encode-entity+vt+tt+tx-id-key-to
                    nil
                    eid
                    vt
                    tt
                    tx-id)
                   (c/->id-buffer content-hash)]
                  [(c/encode-entity+z+tx-id-key-to
                    nil
                    eid
                    z
                    tx-id)
                   (c/->id-buffer content-hash)]])))

(defn- entities-with-range [vt+tt->entity vt-start tt-start vt-end tt-end]
  (set (for [[_ entity-tx] (subseq vt+tt->entity >= [(c/date->reverse-time-ms vt-end)
                                                     (c/date->reverse-time-ms tt-end)])
             :while (<= (compare vt-start (:crux.db/valid-time entity-tx)) 0)
             :when (and (<= (compare tt-start (:crux.tx/tx-time entity-tx)) 0)
                        (<= (compare (:crux.tx/tx-time entity-tx) tt-end) 0))]
         entity-tx)))

(defn- entity-as-of [vt+tt->entity vt tt]
  (first (for [[_ entity-tx] (subseq vt+tt->entity >= [(c/date->reverse-time-ms vt)
                                                       (c/date->reverse-time-ms tt)])
               :when (<= (compare (:crux.tx/tx-time entity-tx) tt) 0)]
           entity-tx)))

(tcct/defspec test-generative-stress-bitemporal-lookup-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (fkv/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index *kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot *kv*)]
                          (->> (for [[vt tt] (concat txs queries)
                                     :let [expected (entity-as-of vt+tt->entity vt tt)
                                           content-hash (c/new-id (:crux.db/content-hash expected))
                                           expected-or-deleted (when-not (= (c/new-id nil) content-hash)
                                                                 expected)]]
                                 (= expected-or-deleted (c/entity-tx->edn (first (idx/entities-at snapshot [eid] vt tt)))))
                               (every? true?)))))))))

(tcct/defspec test-generative-stress-bitemporal-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (fkv/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index *kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot *kv*)]
                          (->> (for [[[vt-start tt-start] [vt-end tt-end]] (partition 2 (concat txs queries))
                                     :let [[vt-start vt-end] (sort [vt-start vt-end])
                                           [tt-start tt-end] (sort [tt-start tt-end])
                                           expected (entities-with-range vt+tt->entity vt-start tt-start vt-end tt-end)
                                           actual (->> (idx/entity-history-range snapshot eid vt-start tt-start vt-end tt-end)
                                                       (map c/entity-tx->edn)
                                                       (set))]]
                                 (= expected actual))
                               (every? true?)))))))))

(tcct/defspec test-generative-stress-bitemporal-start-of-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (fkv/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index *kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot *kv*)]
                          (->> (for [[vt-start tt-start] (concat txs queries)
                                     :let [vt-end (Date. Long/MAX_VALUE)
                                           tt-end (Date. Long/MAX_VALUE)
                                           expected (entities-with-range vt+tt->entity vt-start tt-start vt-end tt-end)
                                           actual (->> (idx/entity-history-range snapshot eid vt-start tt-start vt-end tt-end)
                                                       (map c/entity-tx->edn)
                                                       (set))]]
                                 (= expected actual))
                               (every? true?)))))))))

(tcct/defspec test-generative-stress-bitemporal-end-of-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (fkv/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index *kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot *kv*)]
                          (->> (for [[vt-end tt-end] (concat txs queries)
                                     :let [vt-start (Date. Long/MIN_VALUE)
                                           tt-start (Date. Long/MIN_VALUE)
                                           expected (entities-with-range vt+tt->entity vt-start tt-start vt-end tt-end)
                                           actual (->> (idx/entity-history-range snapshot eid vt-start tt-start vt-end tt-end)
                                                       (map c/entity-tx->edn)
                                                       (set))]]
                                 (= expected actual))
                               (every? true?)))))))))

(tcct/defspec test-generative-stress-bitemporal-full-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)]
                  (fkv/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index *kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot *kv*)]
                          (let [vt-start (Date. Long/MIN_VALUE)
                                tt-start (Date. Long/MIN_VALUE)
                                vt-end (Date. Long/MAX_VALUE)
                                tt-end (Date. Long/MAX_VALUE)
                                expected (entities-with-range vt+tt->entity vt-start tt-start vt-end tt-end)
                                actual (->> (idx/entity-history-range snapshot eid vt-start tt-start vt-end tt-end)
                                            (map c/entity-tx->edn)
                                            (set))]
                            (= expected actual)))))))))

(t/deftest test-can-perform-unary-join
  (let [a-idx (idx/new-relation-virtual-index :a
                                              [[0]
                                               [1]
                                               [3]
                                               [4]
                                               [5]
                                               [6]
                                               [7]
                                               [8]
                                               [9]
                                               [11]
                                               [12]]
                                              1)
        b-idx (idx/new-relation-virtual-index :b
                                              [[0]
                                               [2]
                                               [6]
                                               [7]
                                               [8]
                                               [9]
                                               [12]]
                                              1)
        c-idx (idx/new-relation-virtual-index :c
                                              [[2]
                                               [4]
                                               [5]
                                               [8]
                                               [10]
                                               [12]]
                                              1)]

    (t/is (= [{:x 8}
              {:x 12}]
             (for [[_ join-results] (-> (idx/new-unary-join-virtual-index [(assoc a-idx :name :x)
                                                                           (assoc b-idx :name :x)
                                                                           (assoc c-idx :name :x)])
                                        (idx/idx->seq))]
               join-results)))))

;; Q(a, b, c) ← R(a, b), S(b, c), T (a, c).

;; (1, 3, 4)
;; (1, 3, 5)
;; (1, 4, 6)
;; (1, 4, 8)
;; (1, 4, 9)
;; (1, 5, 2)
;; (3, 5, 2)
;; TODO: Same as above.
(t/deftest test-can-perform-n-ary-join
  (let [r (idx/new-relation-virtual-index :r
                                          [[1 3]
                                           [1 4]
                                           [1 5]
                                           [3 5]]
                                          2)
        s (idx/new-relation-virtual-index :s
                                          [[3 4]
                                           [3 5]
                                           [4 6]
                                           [4 8]
                                           [4 9]
                                           [5 2]]
                                          2)
        t (idx/new-relation-virtual-index :t
                                          [[1 4]
                                           [1 5]
                                           [1 6]
                                           [1 8]
                                           [1 9]
                                           [1 2]
                                           [3 2]]
                                          2)]
    (t/testing "n-ary join"
      (let [index-groups [[(assoc r :name :a) (assoc t :name :a)]
                          [(assoc r :name :b) (assoc s :name :b)]
                          [(assoc s :name :c) (assoc t :name :c)]]
            result (-> (mapv idx/new-unary-join-virtual-index index-groups)
                       (idx/new-n-ary-join-layered-virtual-index)
                       (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                       (idx/layered-idx->seq))]
        (t/is (= [{:a 1, :b 3, :c 4}
                  {:a 1, :b 3, :c 5}
                  {:a 1, :b 4, :c 6}
                  {:a 1, :b 4, :c 8}
                  {:a 1, :b 4, :c 9}
                  {:a 1, :b 5, :c 2}
                  {:a 3, :b 5, :c 2}]
                 (for [[_ join-results] result]
                   join-results)))))))

(t/deftest test-sorted-virtual-index
  (let [idx (idx/new-sorted-virtual-index
             [[(c/->value-buffer 1) :a]
              [(c/->value-buffer 3) :c]])]
    (t/is (= :a
             (second (db/seek-values idx (c/->value-buffer 0)))))
    (t/is (= :a
             (second (db/seek-values idx (c/->value-buffer 1)))))
    (t/is (= :c
             (second (db/next-values idx))))
    (t/is (= :c
             (second (db/seek-values idx (c/->value-buffer 2)))))
    (t/is (= :c
             (second (db/seek-values idx (c/->value-buffer 3)))))
    (t/is (nil? (db/seek-values idx (c/->value-buffer 4))))))

(t/deftest test-range-predicates
  (let [r (idx/new-relation-virtual-index :r
                                          [[1]
                                           [2]
                                           [3]
                                           [4]
                                           [5]]
                                          1)]

    (t/is (= [1 2 3 4 5]
             (->> (idx/idx->seq r)
                  (map second))))

    (t/is (= [1 2 3 4 5]
             (into []
                   (map second)
                   (idx/idx->series r))))

    (t/is (= [1 2 3]
             (->> (idx/idx->seq (idx/new-less-than-virtual-index r 4))
                  (map second))))

    (t/is (= [1 2 3 4]
             (->> (idx/idx->seq (idx/new-less-than-equal-virtual-index r 4))
                  (map second))))

    (t/is (= [3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-virtual-index r 2))
                  (map second))))

    (t/is (= [2 3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-equal-virtual-index r 2))
                  (map second))))

    (t/testing "seek skips to lower range"
      (t/is (= 2 (second (db/seek-values (idx/new-greater-than-equal-virtual-index r 2) (c/->value-buffer nil)))))
      (t/is (= 3 (second (db/seek-values (idx/new-greater-than-virtual-index r 2) (c/->value-buffer 1))))))

    (t/testing "combining indexes"
      (t/is (= [2 3 4]
               (->> (idx/idx->seq (-> r
                                      (idx/new-greater-than-equal-virtual-index 2)
                                      (idx/new-less-than-virtual-index 5)))
                    (map second)))))

    (t/testing "incompatible type"
      (t/is (empty? (->> (idx/idx->seq (-> (idx/new-greater-than-equal-virtual-index r "foo")))
                         (map second)))))))

(t/deftest test-or-virtual-index
  (let [idx-1 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 1) :a]
                [(c/->value-buffer 3) :c]
                [(c/->value-buffer 5) :e1]])
        idx-2 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 2) :b]
                [(c/->value-buffer 4) :d]
                [(c/->value-buffer 5) :e2]
                [(c/->value-buffer 7) :g]])
        idx-3 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 5) :e3]
                [(c/->value-buffer 6) :f]])
        idx (idx/new-or-virtual-index [idx-1 idx-2 idx-3])]
    (t/testing "interleaves results in value order"
      (t/is (= :a
               (second (db/seek-values idx nil))))
      (t/is (= :b
               (second (db/next-values idx))))
      (t/is (= :c
               (second (db/next-values idx))))
      (t/is (= :d
               (second (db/next-values idx)))))
    (t/testing "shared values are returned in index order"
      (t/is (= :e1
               (second (db/next-values idx))))
      (t/is (= :e2
               (second (db/next-values idx))))
      (t/is (= :e3
               (second (db/next-values idx)))))
    (t/testing "can continue after one index is done"
      (t/is (= :f
               (second (db/next-values idx))))
      (t/is (= :g
               (second (db/next-values idx)))))
    (t/testing "returns nil after all indexes are done"
      (t/is (nil? (db/next-values idx))))

    (t/testing "can seek into indexes"
      (t/is (= :d
               (second (db/seek-values idx (c/->value-buffer 4)))))
      (t/is (= :e1
               (second (db/next-values idx)))))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (idx/read-meta *kv* :bar)))
  (idx/store-meta *kv* :bar {:bar 2})
  (t/is (= {:bar 2} (idx/read-meta *kv* :bar)))

  (t/testing "need exact match"
    ;; :bar 0062cdb7020ff920e5aa642c3d4066950dd1f01f4d
    ;; :foo 000beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
    (t/is (nil? (idx/read-meta *kv* :foo)))))

;; NOTE: variable order must align up with relation position order
;; here. This implies that a relation cannot use the same variable
;; twice in two positions. All relations and the join order must be in
;; the same order for it to work.
(t/deftest test-n-ary-join-based-on-relational-tuples
  (let [r-idx (idx/new-relation-virtual-index :r
                                              [[7 4]
                                               ;; extra sanity check
                                               [8 4]]
                                              2)
        s-idx (idx/new-relation-virtual-index :s
                                              [[4 0]
                                               [4 1]
                                               [4 2]
                                               [4 3]]
                                              2)
        t-idx (idx/new-relation-virtual-index :t
                                              [[7 0]
                                               [7 1]
                                               [7 2]
                                               [8 1]
                                               [8 2]]
                                              2)
        index-groups [[(assoc r-idx :name :a)
                       (assoc t-idx :name :a)]
                      [(assoc r-idx :name :b)
                       (assoc s-idx :name :b)]
                      [(assoc s-idx :name :c)
                       (assoc t-idx :name :c)]]]
    (t/is (= #{[7 4 0]
               [7 4 1]
               [7 4 2]
               [8 4 1]
               [8 4 2]}
             (set (for [[_ join-results] (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                             (idx/new-n-ary-join-layered-virtual-index)
                                             (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                             (idx/layered-idx->seq))]
                    (vec (for [var [:a :b :c]]
                           (get join-results var)))))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-unary-conjunction-and-disjunction
  (let [p-idx (idx/new-relation-virtual-index :p
                                              [[1]
                                               [2]
                                               [3]]
                                              1)
        q-idx (idx/new-relation-virtual-index :q
                                              [[2]
                                               [3]
                                               [4]]
                                              1)
        r-idx (idx/new-relation-virtual-index :r
                                              [[3]
                                               [4]
                                               [5]]
                                              1)]
    (t/testing "conjunction"
      (let [unary-and-idx (idx/new-unary-join-virtual-index [(assoc p-idx :name :x)
                                                             (assoc q-idx :name :x)
                                                             (assoc r-idx :name :x)])]
        (t/is (= #{[3]}
                 (set (for [[_ join-results] (-> (idx/new-n-ary-join-layered-virtual-index [unary-and-idx])
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x]]
                               (get join-results var)))))))))

    (t/testing "disjunction"
      (let [unary-or-idx (idx/new-or-virtual-index
                          [(idx/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                           (idx/new-unary-join-virtual-index [(assoc q-idx :name :x)
                                                              (assoc r-idx :name :x)])])]
        (t/is (= #{[1]
                   [2]
                   [3]
                   [4]}
                 (set (for [[_ join-results] (-> (idx/new-n-ary-join-layered-virtual-index [unary-or-idx])
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x]]
                               (get join-results var)))))))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-n-ary-conjunction-and-disjunction
  (let [p-idx (idx/new-relation-virtual-index :p
                                              [[1 3]
                                               [2 4]
                                               [2 20]]
                                              2)
        q-idx (idx/new-relation-virtual-index :q
                                              [[1 10]
                                               [2 20]
                                               [3 30]]
                                              2)
        index-groups [[(assoc p-idx :name :x)
                       (assoc q-idx :name :x)]
                      [(assoc p-idx :name :y)]
                      [(assoc q-idx :name :z)]]]
    (t/testing "conjunction"
      (t/is (= #{[1 3 10]
                 [2 4 20]
                 [2 20 20]}
               (set (for [[_ join-results] (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                               (idx/new-n-ary-join-layered-virtual-index)
                                               (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                               (idx/layered-idx->seq))]
                      (vec (for [var [:x :y :z]]
                             (get join-results var))))))))

    (t/testing "disjunction"
      (let [zero-idx (idx/new-relation-virtual-index :zero
                                                     [[0]]
                                                     1)
            lhs-index (idx/new-n-ary-join-layered-virtual-index
                       [(idx/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                        (idx/new-unary-join-virtual-index [(assoc p-idx :name :y)])
                        (idx/new-unary-join-virtual-index [(assoc zero-idx :name :z)])])]
        (t/is (= #{[1 3 0]
                   [2 4 0]
                   [2 20 0]}
                 (set (for [[_ join-results] (-> lhs-index
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x :y :z]]
                               (get join-results var)))))))
        (let [rhs-index (idx/new-n-ary-join-layered-virtual-index
                         [(idx/new-unary-join-virtual-index [(assoc q-idx :name :x)])
                          (idx/new-unary-join-virtual-index [(assoc zero-idx :name :y)])
                          (idx/new-unary-join-virtual-index [(assoc q-idx :name :z)])])]
          (t/is (= #{[1 0 10]
                     [2 0 20]
                     [3 0 30]}
                   (set (for [[_ join-results] (-> rhs-index
                                                   (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                   (idx/layered-idx->seq))]
                          (vec (for [var [:x :y :z]]
                                 (get join-results var))))))))))))
