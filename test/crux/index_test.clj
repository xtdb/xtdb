(ns crux.index-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [crux.byte-utils :as bu]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures :as f]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [taoensso.nippy :as nippy])
  (:import java.util.Date))

(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store f/with-silent-test-check)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-id %)))))

(t/deftest test-can-store-doc
  (let [object-store (idx/->KvObjectStore f/*kv*)
        tx-log (f/kv-tx-log f/*kv*)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (c/new-id picasso)]
    (t/is (= 48 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (db/submit-doc tx-log content-hash picasso)
    (with-open [snapshot (kv/new-snapshot f/*kv*)]
      (t/is (= {content-hash picasso}
               (db/get-objects object-store snapshot [content-hash])))

      (t/testing "non existent docs are ignored"
        (t/is (= {content-hash picasso}
                 (db/get-objects object-store
                                 snapshot
                                 [content-hash
                                  "090622a35d4b579d2fcfebf823821298711d3867"])))
        (t/is (empty? (db/get-objects object-store snapshot [])))))))

(t/deftest test-can-correct-ranges-in-the-past
  (let [object-store (idx/->KvObjectStore f/*kv*)
        tx-log (tx/->KvTxLog f/*kv* object-store)
        ivan {:crux.db/id :ivan :name "Ivan"}

        v1-ivan (assoc ivan :version 1)
        v1-valid-time #inst "2018-11-26"
        {v1-tx-time :crux.tx/tx-time
         v1-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v1-ivan v1-valid-time]])

        v2-ivan (assoc ivan :version 2)
        v2-valid-time #inst "2018-11-27"
        {v2-tx-time :crux.tx/tx-time
         v2-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v2-ivan v2-valid-time]])

        v3-ivan (assoc ivan :version 3)
        v3-valid-time #inst "2018-11-28"
        {v3-tx-time :crux.tx/tx-time
         v3-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v3-ivan v3-valid-time]])]

    (with-open [snapshot (kv/new-snapshot f/*kv*)]
      (t/testing "first version of entity is visible"
        (t/is (= v1-tx-id (-> (idx/entities-at snapshot [:ivan] v1-valid-time v3-tx-time)
                              (first)
                              :tx-id))))

      (t/testing "second version of entity is visible"
        (t/is (= v2-tx-id (-> (idx/entities-at snapshot [:ivan] v2-valid-time v3-tx-time)
                              (first)
                              :tx-id))))

      (t/testing "third version of entity is visible"
        (t/is (= v3-tx-id (-> (idx/entities-at snapshot [:ivan] v3-valid-time v3-tx-time)
                              (first)
                              :tx-id)))))

    (let [corrected-ivan (assoc ivan :version 4)
          corrected-start-valid-time #inst "2018-11-27"
          corrected-end-valid-time #inst "2018-11-29"
          {corrected-tx-time :crux.tx/tx-time
           corrected-tx-id :crux.tx/tx-id}
          @(db/submit-tx tx-log [[:crux.tx/put :ivan corrected-ivan corrected-start-valid-time corrected-end-valid-time]])]

      (with-open [snapshot (kv/new-snapshot f/*kv*)]
        (t/testing "first version of entity is still there"
          (t/is (= v1-tx-id (-> (idx/entities-at snapshot [:ivan] v1-valid-time corrected-tx-time)
                                (first)
                                :tx-id))))

        (t/testing "second version of entity was corrected"
          (t/is (= {:content-hash (c/new-id corrected-ivan)
                    :tx-id corrected-tx-id}
                   (-> (idx/entities-at snapshot [:ivan] v2-valid-time corrected-tx-time)
                       (first)
                       (select-keys [:tx-id :content-hash])))))

        (t/testing "third version of entity was corrected"
          (t/is (= {:content-hash (c/new-id corrected-ivan)
                    :tx-id corrected-tx-id}
                   (-> (idx/entities-at snapshot [:ivan] v3-valid-time corrected-tx-time)
                       (first)
                       (select-keys [:tx-id :content-hash]))))))

      (let [deleted-start-valid-time #inst "2018-11-25"
            deleted-end-valid-time #inst "2018-11-28"
            {deleted-tx-time :crux.tx/tx-time
             deleted-tx-id :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/delete :ivan deleted-start-valid-time deleted-end-valid-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/testing "first version of entity was deleted"
            (t/is (empty? (idx/entities-at snapshot [:ivan] v1-valid-time deleted-tx-time))))

          (t/testing "second version of entity was deleted"
            (t/is (empty? (idx/entities-at snapshot [:ivan] v2-valid-time deleted-tx-time))))

          (t/testing "third version of entity is still there"
            (t/is (= {:content-hash (c/new-id corrected-ivan)
                      :tx-id corrected-tx-id}
                     (-> (idx/entities-at snapshot [:ivan] v3-valid-time deleted-tx-time)
                         (first)
                         (select-keys [:tx-id :content-hash])))))))

      (t/testing "end range is exclusive"
        (let [{deleted-tx-time :crux.tx/tx-time
               deleted-tx-id :crux.tx/tx-id}
              @(db/submit-tx tx-log [[:crux.tx/delete :ivan v3-valid-time v3-valid-time]])]

          (with-open [snapshot (kv/new-snapshot f/*kv*)]
            (t/testing "third version of entity is still there"
              (t/is (= {:content-hash (c/new-id corrected-ivan)
                        :tx-id corrected-tx-id}
                       (-> (idx/entities-at snapshot [:ivan] v3-valid-time deleted-tx-time)
                           (first)
                           (select-keys [:tx-id :content-hash])))))))))))

;; TODO: This test just shows that this is an issue, if we fix the
;; underlying issue this test should start failing. We can then change
;; the second assertion if we want to keep it around to ensure it
;; keeps working.
(t/deftest test-corrections-in-the-past-slowes-down-bitemp-144
  (let [tx-log (f/kv-tx-log f/*kv*)
        ivan {:crux.db/id :ivan :name "Ivan"}
        start-valid-time #inst "2019"
        number-of-versions 1000]

    @(db/submit-tx tx-log (vec (for [n (range number-of-versions)]
                                 [:crux.tx/put :ivan (assoc ivan :verison n) (Date. (+ (.getTime start-valid-time) (inc (long n))))])))

    (with-open [snapshot (kv/new-snapshot f/*kv*)]
      (let [baseline-time (let [start-time (System/nanoTime)
                                valid-time (Date. (+ (.getTime start-valid-time) number-of-versions))]
                            (t/testing "last version of entity is visible at now"
                              (t/is (= valid-time (-> (idx/entities-at snapshot [:ivan] valid-time (Date.))
                                                      (first)
                                                      :vt))))
                            (- (System/nanoTime) start-time))]

        (let [start-time (System/nanoTime)
              valid-time (Date. (+ (.getTime start-valid-time) number-of-versions))]
          (t/testing "no version is visible before transactions"
            (t/is (nil? (idx/entities-at snapshot [:ivan] valid-time valid-time)))
            (let [corrections-time (- (System/nanoTime) start-time)]
              ;; TODO: This can be a bit flaky. This assertion was
              ;; mainly there to prove the opposite, but it has been
              ;; fixed. Can be added back to sanity check when
              ;; changing indexes.
              #_(t/is (>= baseline-time corrections-time)))))))))

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
                  (f/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index f/*kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                  (f/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index f/*kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                  (f/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index f/*kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                  (f/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index f/*kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                  (f/with-kv-store
                    (fn []
                      (let [vt+tt->entity (vt+tt+deleted?->vt+tt->entities eid txs)]
                        (write-vt+tt->entities-direct-to-index f/*kv* vt+tt->entity)
                        (with-open [snapshot (kv/new-snapshot f/*kv*)]
                          (let [vt-start (Date. Long/MIN_VALUE)
                                tt-start (Date. Long/MIN_VALUE)
                                vt-end (Date. Long/MAX_VALUE)
                                tt-end (Date. Long/MAX_VALUE)
                                expected (entities-with-range vt+tt->entity vt-start tt-start vt-end tt-end)
                                actual (->> (idx/entity-history-range snapshot eid vt-start tt-start vt-end tt-end)
                                            (map c/entity-tx->edn)
                                            (set))]
                            (= expected actual)))))))))

(t/deftest test-can-read-kv-tx-log
  (let [tx-log (f/kv-tx-log f/*kv*)
        ivan {:crux.db/id :ivan :name "Ivan"}

        tx1-ivan (assoc ivan :version 1)
        tx1-valid-time #inst "2018-11-26"
        {tx1-id :crux.tx/tx-id
         tx1-tx-time :crux.tx/tx-time}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan tx1-ivan tx1-valid-time]])

        tx2-ivan (assoc ivan :version 2)
        tx2-petr {:crux.db/id :petr :name "Petr"}
        tx2-valid-time #inst "2018-11-27"
        {tx2-id :crux.tx/tx-id
         tx2-tx-time :crux.tx/tx-time}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan tx2-ivan tx2-valid-time]
                               [:crux.tx/put :petr tx2-petr tx2-valid-time]])]

    (with-open [tx-log-context (db/new-tx-log-context tx-log)]
      (let [log (db/tx-log tx-log tx-log-context nil)]
        (t/is (not (realized? log)))
        (t/is (= [{:crux.tx/tx-id tx1-id
                   :crux.tx/tx-time tx1-tx-time
                   :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id tx1-ivan) tx1-valid-time]]}
                  {:crux.tx/tx-id tx2-id
                   :crux.tx/tx-time tx2-tx-time
                   :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id tx2-ivan) tx2-valid-time]
                                    [:crux.tx/put (c/new-id :petr) (c/new-id tx2-petr) tx2-valid-time]]}]
                 log))))))

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
  (t/is (nil? (idx/read-meta f/*kv* :bar)))
  (idx/store-meta f/*kv* :bar {:bar 2})
  (t/is (= {:bar 2} (idx/read-meta f/*kv* :bar)))

  (t/testing "need exact match"
    ;; :bar 0062cdb7020ff920e5aa642c3d4066950dd1f01f4d
    ;; :foo 000beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
    (t/is (nil? (idx/read-meta f/*kv* :foo)))))

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
