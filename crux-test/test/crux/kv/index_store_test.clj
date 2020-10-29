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
  (:import crux.codec.EntityTx
           crux.api.NodeOutOfSyncException
           java.util.Date))

(def ^:dynamic *index-store*)

(t/use-fixtures :each fkv/with-each-kv-store* f/with-silent-test-check)

(defmacro with-fresh-index-store [& body]
  `(fkv/with-kv-store [kv-store#]
     (binding [*index-store* (kvi/->KvIndexStore kv-store# (nop-cache/->nop-cache {}) (nop-cache/->nop-cache {}))]
       ~@body)))

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

(defn- vt+tt+deleted?->vt+tt->etx [eid txs]
  (->> (for [[tx-id [vt tt deleted?]] (map-indexed vector (sort-by second txs))]
         [[(c/date->reverse-time-ms vt)
           (c/date->reverse-time-ms tt)]
          (c/->EntityTx (c/new-id eid)
                        vt
                        tt
                        tx-id
                        (if deleted?
                          (c/new-id nil)
                          (c/new-id (keyword (str tx-id)))))])
       (into (sorted-map))))

(defn- write-etxs [etxs]
  (doseq [[[tx-time tx-id] etxs] (->> (group-by (juxt :tt :tx-id) etxs)
                                      (sort-by (comp second key)))]
    (db/index-entity-txs *index-store* {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time} etxs)))

(defn- entities-with-range [vt+tt->etx vt-start tt-start vt-end tt-end]
  (->> (subseq vt+tt->etx >= [(c/date->reverse-time-ms vt-start)
                                 (c/date->reverse-time-ms tt-start)])
       vals
       (take-while #(pos? (compare (.vt ^EntityTx %) vt-end)))
       (filter #(pos? (compare (.tt ^EntityTx %) tt-end)))
       (remove #(pos? (compare (.tt ^EntityTx %) tt-start)))
       set))

(defn- entity-as-of ^crux.codec.EntityTx [vt+tt->entity vt tt]
  (->> (subseq vt+tt->entity >= [(c/date->reverse-time-ms vt)
                                 (c/date->reverse-time-ms tt)])
       vals
       (remove #(pos? (compare (.tt ^EntityTx %) tt)))
       first))

(tcct/defspec test-generative-stress-bitemporal-lookup-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date) 100)]
                  (with-fresh-index-store
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)
                          max-tt (->> (map second txs) sort last)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [[vt tt] (concat txs queries)
                                   :when (not (pos? (compare tt max-tt)))
                                   :let [tx-id (:crux.tx/tx-id (db/resolve-tx index-snapshot {:crux.tx/tx-time tt}))]
                                   :when tx-id]
                               (= (entity-as-of vt+tt->etx vt tt)
                                  (db/entity-as-of index-snapshot eid vt tx-id)))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (with-fresh-index-store
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [[[vt1 tt1] [vt2 tt2]] (partition 2 (concat txs queries))
                                   :let [[vt-end vt-start] (sort [vt1 vt2])
                                         [tt-end tt-start] (sort [tt1 tt2])
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-snapshot
                                                                        eid :desc
                                                                        {:start {:crux.db/valid-time vt-start
                                                                                 :crux.tx/tx-time tt-start}
                                                                         :end {:crux.db/valid-time vt-end
                                                                               :crux.tx/tx-time tt-end}})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-start-of-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (with-fresh-index-store
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [[vt-start tt-start] (concat txs queries)
                                   :let [vt-end (Date. Long/MIN_VALUE)
                                         tt-end (Date. Long/MIN_VALUE)
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-snapshot eid :desc
                                                                        {:start {:crux.db/valid-time vt-start
                                                                                 :crux.tx/tx-time tt-start}})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-end-of-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (with-fresh-index-store
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (->> (for [[vt-end tt-end] (concat txs queries)
                                   :let [vt-start (Date. Long/MAX_VALUE)
                                         tt-start (Date. Long/MAX_VALUE)
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-snapshot eid :desc
                                                                        {:end {:crux.db/valid-time vt-end
                                                                               :crux.tx/tx-time tt-end}})
                                                     (set))]]
                               (= expected actual))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-full-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)]
                  (with-fresh-index-store
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-snapshot (db/open-index-snapshot *index-store*)]
                        (let [vt-start (Date. Long/MAX_VALUE)
                              tt-start (Date. Long/MAX_VALUE)
                              vt-end (Date. Long/MIN_VALUE)
                              tt-end (Date. Long/MIN_VALUE)
                              expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
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
      (db/index-entity-txs *index-store* tx0 [])
      (db/index-entity-txs *index-store* tx1 [])

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

        ;; This is an edge case - arguably it should throw IAE,
        ;;   but instead returns an instant with tx-time "2022", but before tx-id 1 was ingested.
        ;; The query engine would still behave consistently in this case (with tx-id = 0),
        ;;   and it's unlikely that a user will hit it unless they explicitly ask for it,
        ;;   so it's a philosophical question as to whether we should allow it or check for it :)
        (t/is (= #::tx{:tx-time #inst "2022", :tx-id 0}
                 (db/resolve-tx index-snapshot #::tx{:tx-time #inst "2022", :tx-id 0})))

        (t/is (thrown? NodeOutOfSyncException
                       (db/resolve-tx index-snapshot #::tx{:tx-time #inst "2023", :tx-id 1})))))))
