(ns crux.kv.index-store-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.cache.nop :as nop-cache]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.kv :as fkv]
            [crux.kv.index-store :as kvi]
            [crux.tx :as tx])
  (:import clojure.lang.MapEntry
           crux.codec.EntityTx
           crux.api.NodeOutOfSyncException
           [java.util Date UUID]))

(def ^:dynamic *index-store*)

(t/use-fixtures :each fkv/with-each-kv-store* f/with-silent-test-check)

(defmacro with-fresh-index-store [& body]
  `(fkv/with-kv-store [kv-store#]
     (binding [*index-store* (kvi/->kv-index-store {:kv-store kv-store#
                                                    :cav-cache  (nop-cache/->nop-cache {})
                                                    :canonical-buffer-cache (nop-cache/->nop-cache {})})]
       ~@body)))

;; NOTE: These tests does not go via the TxLog, but writes its own
;; transactions direct into the KV store so it can generate random
;; histories of both valid and transaction time.

(defn gen-date [start-date end-date]
  (->> (gen/choose (inst-ms start-date) (inst-ms end-date))
       (gen/fmap #(Date. (long %)))))

(defn gen-vt+tid+deleted? [{:keys [start-vt end-vt start-tid end-tid]}]
  (->> (gen/tuple (gen-date start-vt end-vt)
                  (gen/choose start-tid end-tid)
                  (gen/frequency [[8 (gen/return false)]
                                  [2 (gen/return true)]]))
       (gen/fmap #(zipmap [:valid-time :tx-id :deleted?] %))))

(defn gen-query-vt+tid [{:keys [start-vt end-vt start-tid end-tid]}]
  (->> (gen/tuple (gen-date start-vt end-vt)
                  (gen/choose start-tid end-tid))
       (gen/fmap #(zipmap [:valid-time :tx-id] %))))

(defn- vt+tid+deleted?->vt+tid->etx [eid txs]
  (->> (for [{:keys [valid-time ^long tx-id deleted?]} txs]
         [[(c/date->reverse-time-ms valid-time)
           (c/descending-long tx-id)]
          (c/->EntityTx (c/new-id eid)
                        valid-time
                        (Date. tx-id)
                        tx-id
                        (if deleted?
                          (c/new-id nil)
                          (c/new-id (keyword (str tx-id)))))])
       (into (sorted-map))))

(defn- write-etxs [etxs]
  (doseq [[^long tx-id etxs] (->> (group-by :tx-id etxs)
                                  (sort-by key))]
    (doto (db/begin-index-tx *index-store* {:crux.tx/tx-id tx-id, :crux.tx/tx-time (Date. tx-id)} nil)
      (db/index-entity-txs etxs)
      (db/commit-index-tx))))

(defn- entities-with-range [vt+tid->etx {:keys [start-vt end-vt start-tid end-tid]}]
  (->> (subseq vt+tid->etx >= [(c/date->reverse-time-ms start-vt)
                               (c/descending-long start-tid)])
       vals
       (take-while #(pos? (compare (.vt ^EntityTx %) end-vt)))
       (filter #(pos? (compare (.tx-id ^EntityTx %) end-tid)))
       (remove #(pos? (compare (.tx-id ^EntityTx %) start-tid)))
       set))

(defn- entity-as-of ^crux.codec.EntityTx [vt+tid->entity vt tx-id]
  (->> (subseq vt+tid->entity >= [(c/date->reverse-time-ms vt)
                                  (c/descending-long tx-id)])
       vals
       (remove #(pos? (compare (.tx-id ^EntityTx %) tx-id)))
       first))

(tcct/defspec test-generative-stress-bitemporal-lookup-test 50
  (let [eid (c/->id-buffer :foo)]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tid+deleted? {:start-vt #inst "2019", :start-tid 0, :end-vt #inst "2020", :end-tid 100}) 50)
                   queries (gen/vector (gen-query-vt+tid {:start-vt #inst "2018", :start-tid 0, :end-vt #inst "2021", :end-tid 100}) 100)]
                  (with-fresh-index-store
                    (let [vt+tid->etx (vt+tid+deleted?->vt+tid->etx eid txs)]
                      (write-etxs (vals vt+tid->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [{:keys [valid-time tx-id]} (concat txs queries)]
                               (= (entity-as-of vt+tid->etx valid-time tx-id)
                                  (db/entity-as-of index-snapshot eid valid-time tx-id)))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-range-test 50
  (let [eid (c/->id-buffer :foo)]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tid+deleted? {:start-vt #inst "2019", :start-tid 0, :end-vt #inst "2020", :end-tid 100}) 50)
                   queries (gen/vector (gen-query-vt+tid {:start-vt #inst "2018", :start-tid 0, :end-vt #inst "2021", :end-tid 100}) 100)]
                  (with-fresh-index-store
                    (let [vt+tid->etx (vt+tid+deleted?->vt+tid->etx eid txs)]
                      (write-etxs (vals vt+tid->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [time-pair (partition 2 (concat txs queries))
                                   :let [[end-vt start-vt] (sort (map :valid-time time-pair))
                                         [end-tid start-tid] (sort (map :tx-id time-pair))
                                         expected (entities-with-range vt+tid->etx
                                                                       {:start-vt start-vt, :start-tid start-tid
                                                                        :end-vt end-vt, :end-tid end-tid})
                                         actual (->> (db/entity-history index-snapshot
                                                                        eid :desc
                                                                        {:start-valid-time start-vt
                                                                         :start-tx-id start-tid
                                                                         :end-valid-time end-vt
                                                                         :end-tx-id end-tid})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-start-of-range-test 50
  (let [eid (c/->id-buffer :foo)]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tid+deleted? {:start-vt #inst "2019", :start-tid 0, :end-vt #inst "2020", :end-tid 100}) 50)
                   queries (gen/vector (gen-query-vt+tid {:start-vt #inst "2018", :start-tid 0, :end-vt #inst "2021", :end-tid 100}) 100)]
                  (with-fresh-index-store
                    (let [vt+tid->etx (vt+tid+deleted?->vt+tid->etx eid txs)]
                      (write-etxs (vals vt+tid->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [{:keys [valid-time tx-id]} (concat txs queries)
                                   :let [expected (entities-with-range vt+tid->etx
                                                                       {:start-vt valid-time, :start-tid tx-id
                                                                        :end-vt (Date. Long/MIN_VALUE), :end-tid Long/MIN_VALUE})
                                         actual (->> (db/entity-history index-snapshot
                                                                        eid :desc
                                                                        {:start-valid-time valid-time
                                                                         :start-tx-id tx-id})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-end-of-range-test 50
  (let [eid (c/->id-buffer :foo)]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tid+deleted? {:start-vt #inst "2019", :start-tid 0, :end-vt #inst "2020", :end-tid 100}) 50)
                   queries (gen/vector (gen-query-vt+tid {:start-vt #inst "2018", :start-tid 0, :end-vt #inst "2021", :end-tid 100}) 100)]
                  (with-fresh-index-store
                    (let [vt+tid->etx (vt+tid+deleted?->vt+tid->etx eid txs)]
                      (write-etxs (vals vt+tid->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [{:keys [valid-time tx-id]} (concat txs queries)
                                   :let [expected (entities-with-range vt+tid->etx
                                                                       {:start-vt (Date. Long/MAX_VALUE), :start-tid Long/MAX_VALUE
                                                                        :end-vt valid-time, :end-tid tx-id})
                                         actual (->> (db/entity-history index-snapshot
                                                                        eid :desc
                                                                        {:end-valid-time valid-time
                                                                         :end-tx-id tx-id})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-full-range-test 50
  (let [eid (c/->id-buffer :foo)]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tid+deleted? {:start-vt #inst "2019", :start-tid 0, :end-vt #inst "2020", :end-tid 100}) 50)]
                  (with-fresh-index-store
                    (let [vt+tid->etx (vt+tid+deleted?->vt+tid->etx eid txs)]
                      (write-etxs (vals vt+tid->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (let [expected (entities-with-range vt+tid->etx
                                                            {:start-vt (Date. Long/MAX_VALUE), :start-tid Long/MAX_VALUE
                                                             :end-vt (Date. Long/MIN_VALUE), :end-tid Long/MIN_VALUE})
                              actual (->> (db/entity-history index-snapshot eid :desc {})
                                          (set))]
                          (= expected actual))))))))

(t/deftest test-store-and-retrieve-meta
  (with-fresh-index-store
    (t/is (nil? (db/read-index-meta *index-store* :bar)))
    (db/store-index-meta *index-store* :bar {:bar 2})
    (t/is (= {:bar 2} (db/read-index-meta *index-store* :bar)))

    (t/testing "need exact match"
      ;; :bar 0062cdb7020ff920e5aa642c3d4066950dd1f01f4d
      ;; :foo 000beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
      (t/is (nil? (db/read-index-meta *index-store* :foo))))))

(t/deftest test-resolve-tx
  (with-fresh-index-store
    (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
      (t/is (nil? (db/resolve-tx index-snapshot nil)))
      (t/is (thrown? NodeOutOfSyncException (db/resolve-tx index-snapshot {:crux.tx/tx-time (Date.)})))
      (t/is (thrown? NodeOutOfSyncException (db/resolve-tx index-snapshot {:crux.tx/tx-id 1}))))

    (let [tx0 {:crux.tx/tx-time #inst "2020", :crux.tx/tx-id 0}
          tx1 {:crux.tx/tx-time #inst "2022", :crux.tx/tx-id 1}]
      (doto (db/begin-index-tx *index-store* tx0 nil)
        (db/index-entity-txs [])
        (db/commit-index-tx))
      (doto (db/begin-index-tx *index-store* tx1 nil)
        (db/index-entity-txs [])
        (db/commit-index-tx))

      (t/is (= tx1 (db/latest-completed-tx *index-store*)))

      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
        (t/is (= tx0 (db/resolve-tx index-snapshot tx0)))
        (t/is (= tx1 (db/resolve-tx index-snapshot tx1)))
        (t/is (= tx1 (db/resolve-tx index-snapshot nil)))

        (let [tx #::tx{:tx-time #inst "2021", :tx-id 0}]
          (t/is (= tx (db/resolve-tx index-snapshot tx)))
          (t/is (= tx (db/resolve-tx index-snapshot {::tx/tx-time #inst "2021"}))))

        (t/is (thrown? IllegalArgumentException
                       (db/resolve-tx index-snapshot #::tx{:tx-time #inst "2021", :tx-id 1})))

        (t/is (thrown? IllegalArgumentException
                       (db/resolve-tx index-snapshot #::tx{:tx-time #inst "2022", :tx-id 0})))

        (t/is (thrown? NodeOutOfSyncException
                       (db/resolve-tx index-snapshot #::tx{:tx-time #inst "2023", :tx-id 1})))))))

(t/deftest test-statistics
  (letfn [(->stats [index-snapshot-factory]
            (with-open [index-snapshot (db/open-index-snapshot index-snapshot-factory)]
              (->> (db/all-attrs index-snapshot)
                   (into {} (map (juxt identity
                                       (fn [attr]
                                         {:doc-count (db/doc-count index-snapshot attr)
                                          :values (Math/round (db/value-cardinality index-snapshot attr))
                                          :eids (Math/round (db/eid-cardinality index-snapshot attr))})))))))]
    (with-fresh-index-store
      (let [ivan {:crux.db/id :ivan :name "Ivan"}
            ivan2 {:crux.db/id :ivan :name "Ivan2"}
            petr {:crux.db/id :petr :name "Petr"}]
        (let [index-store-tx (db/begin-index-tx *index-store* #::tx{:tx-time #inst "2021", :tx-id 0} nil)]

          (db/index-docs index-store-tx {(c/new-id ivan) ivan})
          (t/is (= {:doc-count 1, :values 1, :eids 1} (:name (->stats index-store-tx))))

          (db/index-docs index-store-tx {(c/new-id petr) petr})
          (t/is (= {:doc-count 2, :values 2, :eids 2} (:name (->stats index-store-tx))))

          (db/commit-index-tx index-store-tx))

        (t/is (= {:doc-count 2, :values 2, :eids 2} (:name (->stats *index-store*))))

        (t/testing "updated"
          (doto (db/begin-index-tx *index-store* #::tx{:tx-time #inst "2022", :tx-id 1} nil)
            (db/index-docs {(c/new-id ivan2) ivan2})
            (db/commit-index-tx))

          (t/is (= {:doc-count 3, :values 3, :eids 2} (:name (->stats *index-store*)))))))

    (with-fresh-index-store
      (let [id-iterator (.iterator ^Iterable (range))]
        (letfn [(mk-docs [n]
                  (for [idx (range n)
                        sub-idx (range idx)]
                    (let [doc {:crux.db/id (.next id-iterator)
                               :sub-idx sub-idx}]
                      (MapEntry/create (c/new-id doc) doc))))]
          (doto (db/begin-index-tx *index-store* #::tx{:tx-time #inst "2021", :tx-id 0} nil)

            (db/index-docs (mk-docs 50))
            (db/index-docs (mk-docs 50))

            (db/commit-index-tx))

          (doto (db/begin-index-tx *index-store* #::tx{:tx-time #inst "2022", :tx-id 1} nil)
            (db/index-docs (mk-docs 50))
            (db/commit-index-tx))))

      (t/is (= {:crux.db/id {:doc-count 3675, :values 3554, :eids 3554}
                :sub-idx {:doc-count 3675, :values 50, :eids 3554}}
               (->stats *index-store*))))))
