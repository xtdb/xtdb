(ns crux.kv.indexer-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.kv :as fkv]
            [crux.kv.indexer :as kvi]
            [crux.tx :as tx])
  (:import crux.codec.EntityTx
           java.util.Date))

(def ^:dynamic *indexer*)

(t/use-fixtures :each fkv/with-each-kv-store* f/with-silent-test-check)

(defmacro with-fresh-indexer [& body]
  `(fkv/with-kv-store [kv-store#]
     (binding [*indexer* (kvi/->KvIndexer kv-store#)]
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
  (db/index-entity-txs *indexer* {:crux.tx/tx-id Long/MAX_VALUE, :crux.tx/tx-time (Date.)} etxs))

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
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (with-fresh-indexer
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]

                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-store (db/open-index-store *indexer*)]
                        (->> (for [[vt tt] (concat txs queries)]
                               (= (entity-as-of vt+tt->etx vt tt)
                                  (db/entity-as-of index-store eid vt tt)))
                             (every? true?))))))))

(tcct/defspec test-generative-stress-bitemporal-range-test 50
  (let [eid (c/->id-buffer :foo)
        start-date #inst "2019"
        end-date #inst "2020"
        query-start-date #inst "2018"
        query-end-date #inst "2021"]
    (prop/for-all [txs (gen/vector-distinct-by second (gen-vt+tt+deleted? start-date end-date) 50)
                   queries (gen/vector (gen-query-vt+tt query-start-date query-end-date)) 100]
                  (with-fresh-indexer
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-store (db/open-index-store *indexer*)]
                        (->> (for [[[vt1 tt1] [vt2 tt2]] (partition 2 (concat txs queries))
                                   :let [[vt-end vt-start] (sort [vt1 vt2])
                                         [tt-end tt-start] (sort [tt1 tt2])
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-store
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
                  (with-fresh-indexer
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-store (db/open-index-store *indexer*)]
                        (->> (for [[vt-start tt-start] (concat txs queries)
                                   :let [vt-end (Date. Long/MIN_VALUE)
                                         tt-end (Date. Long/MIN_VALUE)
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-store eid :desc
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
                  (with-fresh-indexer
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-store (db/open-index-store *indexer*)]
                        (->> (for [[vt-end tt-end] (concat txs queries)
                                   :let [vt-start (Date. Long/MAX_VALUE)
                                         tt-start (Date. Long/MAX_VALUE)
                                         expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                                         actual (->> (db/entity-history index-store eid :desc
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
                  (with-fresh-indexer
                    (let [vt+tt->etx (vt+tt+deleted?->vt+tt->etx eid txs)]
                      (write-etxs (vals vt+tt->etx))
                      (with-open [index-store (db/open-index-store *indexer*)]
                        (let [vt-start (Date. Long/MAX_VALUE)
                              tt-start (Date. Long/MAX_VALUE)
                              vt-end (Date. Long/MIN_VALUE)
                              tt-end (Date. Long/MIN_VALUE)
                              expected (entities-with-range vt+tt->etx vt-start tt-start vt-end tt-end)
                              actual (->> (db/entity-history index-store eid :desc {})
                                          (set))]
                          (= expected actual))))))))

(t/deftest test-store-and-retrieve-meta
  (with-fresh-indexer
    (t/is (nil? (db/read-index-meta *indexer* :bar)))
    (db/store-index-meta *indexer* :bar {:bar 2})
    (t/is (= {:bar 2} (db/read-index-meta *indexer* :bar)))

    (t/testing "need exact match"
      ;; :bar 0062cdb7020ff920e5aa642c3d4066950dd1f01f4d
      ;; :foo 000beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33
      (t/is (nil? (db/read-index-meta *indexer* :foo))))))
