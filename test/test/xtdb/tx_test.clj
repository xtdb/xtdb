(ns xtdb.tx-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :as t]
            [clojure.tools.logging.impl :as log-impl]
            [xtdb.fixtures.kv :as fkv]
            [xtdb.tx.event :as xte]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.rdf :as rdf]
            [xtdb.system :as sys]
            [xtdb.tx :as tx]
            [xtdb.tx.event :as txe])
  (:import [clojure.lang ExceptionInfo Keyword PersistentArrayMap]
           [java.net URI URL]
           java.time.Duration
           [java.util Collections Date HashMap HashSet UUID]))

(t/use-fixtures :each fkv/with-each-kv-store* fkv/with-kv-store-opts* fix/with-node fix/with-silent-test-check
  (fn [f]
    (f)
    (#'tx/reset-tx-fn-error)))

(def picasso-id (keyword "http://dbpedia.org/resource/Pablo_Picasso"))
(def picasso-eid (c/new-id picasso-id))

(def picasso
  (-> "xtdb/Pablo_Picasso.ntriples"
      (rdf/ntriples)
      (rdf/->default-language)
      (rdf/->maps-by-id)
      (get picasso-id)))

;; TODO: This is a large, useful, test that exercises many parts, but
;; might be better split up.
(t/deftest test-can-index-tx-ops-acceptance-test
  (let [content-hash (c/hash-doc picasso)
        valid-time #inst "2018-05-21"
        {::xt/keys [tx-time tx-id]}
        (fix/submit+await-tx [[::xt/put picasso valid-time]])]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/testing "can see entity at transact and valid time"
        (t/is (= (c/map->EntityTx {:eid picasso-eid
                                   :content-hash content-hash
                                   :vt valid-time
                                   :tt tx-time
                                   :tx-id tx-id})
                 (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") tx-time tx-id))))

      (t/testing "cannot see entity before valid or transact time"
        (t/is (nil? (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") #inst "2018-05-20" tx-id)))
        (t/is (nil? (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") tx-time -1))))

      (t/testing "can see entity after valid or transact time"
        (t/is (some? (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") #inst "2018-05-22" tx-id)))
        (t/is (some? (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") tx-time tx-id))))

      (t/testing "can see entity history"
        (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                    :content-hash content-hash
                                    :vt valid-time
                                    :tt tx-time
                                    :tx-id tx-id})]
                 (db/entity-history index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") :desc {})))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (c/hash-doc new-picasso)
            new-valid-time #inst "2018-05-20"
            {new-tx-time ::xt/tx-time
             new-tx-id   ::xt/tx-id}
            (fix/submit+await-tx [[::xt/put new-picasso new-valid-time]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash new-content-hash
                                     :vt new-valid-time
                                     :tt new-tx-time
                                     :tx-id new-tx-id})
                   (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time new-tx-id)))

          (t/is (nil? (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") #inst "2018-05-20" -1))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (c/hash-doc new-picasso)
            new-valid-time #inst "2018-05-22"
            {new-tx-time ::xt/tx-time
             new-tx-id   ::xt/tx-id}
            (fix/submit+await-tx [[::xt/put new-picasso new-valid-time]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash new-content-hash
                                     :vt new-valid-time
                                     :tt new-tx-time
                                     :tx-id new-tx-id})
                   (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time new-tx-id)))
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash content-hash
                                     :vt valid-time
                                     :tt tx-time
                                     :tx-id tx-id})
                   (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time tx-id))))

        (t/testing "can correct entity at earlier valid time"
          (let [new-picasso (assoc picasso :bar :foo)
                new-content-hash (c/hash-doc new-picasso)
                prev-tx-time new-tx-time
                prev-tx-id new-tx-id
                new-valid-time #inst "2018-05-22"
                {new-tx-time ::xt/tx-time
                 new-tx-id   ::xt/tx-id}
                (fix/submit+await-tx [[::xt/put new-picasso new-valid-time]])]

            (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
              (t/is (= (c/map->EntityTx {:eid picasso-eid
                                         :content-hash new-content-hash
                                         :vt new-valid-time
                                         :tt new-tx-time
                                         :tx-id new-tx-id})
                       (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time new-tx-id)))

              (t/is (= prev-tx-id
                       (:tx-id (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") prev-tx-time prev-tx-id)))))))

        (t/testing "can delete entity"
          (let [new-valid-time #inst "2018-05-23"
                {new-tx-id ::xt/tx-id}
                (fix/submit+await-tx [[::xt/delete (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time]])]
            (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
              (t/is (nil? (.content-hash (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") new-valid-time new-tx-id))))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (:tx-id (db/entity-as-of index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") valid-time new-tx-id))))))))))

    (t/testing "can retrieve history of entity"
      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= 5 (count (db/entity-history index-snapshot (keyword "http://dbpedia.org/resource/Pablo_Picasso") :desc
                                             {:with-corrections? true}))))))))

(t/deftest test-can-cas-entity
  (let [{picasso-tx-time ::xt/tx-time, picasso-tx-id ::xt/tx-id} (xt/submit-tx *api* [[::xt/put picasso]])]

    (t/testing "compare and set does nothing with wrong content hash"
      (let [wrong-picasso (assoc picasso :baz :boz)
            cas-failure-tx (xt/submit-tx *api* [[::xt/cas wrong-picasso (assoc picasso :foo :bar)]])]

        (xt/await-tx *api* cas-failure-tx (Duration/ofMillis 1000))

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {}))))))

    (t/testing "compare and set updates with correct content hash"
      (let [new-picasso (assoc picasso :new? true)
            {new-tx-time ::xt/tx-time, new-tx-id ::xt/tx-id} (fix/submit+await-tx [[::xt/cas picasso new-picasso]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc new-picasso)
                                      :vt new-tx-time
                                      :tt new-tx-time
                                      :tx-id new-tx-id})
                    (c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {})))))))

  (t/testing "compare and set can update non existing nil entity"
    (let [ivan {:xt/id :ivan, :value 12}
          {ivan-tx-time ::xt/tx-time, ivan-tx-id ::xt/tx-id} (fix/submit+await-tx [[::xt/cas nil ivan]])]

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= [(c/map->EntityTx {:eid (c/new-id :ivan)
                                    :content-hash (c/hash-doc ivan)
                                    :vt ivan-tx-time
                                    :tt ivan-tx-time
                                    :tx-id ivan-tx-id})]
                 (db/entity-history index-snapshot :ivan :desc {})))))))

(t/deftest test-match-ops
  (let [{picasso-tx-time ::xt/tx-time, picasso-tx-id ::xt/tx-id} (xt/submit-tx *api* [[::xt/put picasso]])]

    (t/testing "match does nothing with wrong content hash"
      (let [wrong-picasso (assoc picasso :baz :boz)
            match-failure-tx (xt/submit-tx *api* [[::xt/match picasso-id wrong-picasso]])]

        (xt/await-tx *api* match-failure-tx (Duration/ofMillis 1000))

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {}))))))

    (t/testing "match continues with correct content hash"
      (let [new-picasso (assoc picasso :new? true)
            {new-tx-time ::xt/tx-time, new-tx-id ::xt/tx-id} (fix/submit+await-tx [[::xt/match picasso-id picasso]
                                                                                   [::xt/put new-picasso]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc new-picasso)
                                      :vt new-tx-time
                                      :tt new-tx-time
                                      :tx-id new-tx-id})
                    (c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/hash-doc picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {})))))))

  (t/testing "match can check non existing entity"
    (let [ivan {:xt/id :ivan, :value 12}
          {ivan-tx-time ::xt/tx-time, ivan-tx-id ::xt/tx-id} (fix/submit+await-tx [[::xt/match :ivan nil]
                                                                                   [::xt/put ivan]])]

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= [(c/map->EntityTx {:eid (c/new-id :ivan)
                                    :content-hash (c/hash-doc ivan)
                                    :vt ivan-tx-time
                                    :tt ivan-tx-time
                                    :tx-id ivan-tx-id})]
                 (db/entity-history index-snapshot :ivan :desc {})))))))

(t/deftest test-can-evict-entity
  (t/testing "removes all traces of entity from indices"
    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :value 0}]])
    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :value 1}]])
    (fix/submit+await-tx [[::xt/evict :foo]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [history (db/entity-history index-snapshot :foo :desc {})]
        (t/testing "eviction removes tx history"
          (t/is (empty? history)))
        (t/testing "eviction removes docs"
          (t/is (empty? (->> (db/fetch-docs (:document-store *api*) (keep :content-hash history))
                             vals
                             (remove c/evicted-doc?))))))))

  (t/testing "clears entity history for valid-time ranges"
    (fix/submit+await-tx [[::xt/put {:xt/id :bar, :value 0} #inst "2012" #inst "2018"]])
    (fix/submit+await-tx [[::xt/evict :bar]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [history (db/entity-history index-snapshot :bar :desc {})]
        (t/testing "eviction removes tx history"
          (t/is (empty? history)))))))

(defn index-tx [tx tx-events docs]
  (let [{:keys [xtdb/tx-indexer]} @(:!system *api*)
        in-flight-tx (db/begin-tx tx-indexer)
        tx (assoc tx ::xte/tx-events tx-events)]

    (db/index-tx-docs in-flight-tx docs)
    (db/index-tx-events in-flight-tx tx)
    (db/commit in-flight-tx tx)))

(t/deftest test-handles-legacy-evict-events
  (let [{put-tx-time ::xt/tx-time, put-tx-id ::xt/tx-id} (fix/submit+await-tx [[::xt/put picasso #inst "2018-05-21"]])

        evict-tx-time #inst "2018-05-22"
        evict-tx-id (inc put-tx-id)

        index-evict! (fn []
                       (index-tx {::xt/tx-time evict-tx-time
                                  ::xt/tx-id evict-tx-id}
                                 [[:crux.tx/evict picasso-id #inst "2018-05-23"]]
                                 {}))]

    ;; we have to index these manually because the new evict API won't allow docs
    ;; with the legacy valid-time range
    (t/testing "eviction throws if legacy params and no explicit override"
      (t/is (thrown-with-msg? IllegalArgumentException
                              #"^Evict no longer supports time-range parameters."
                              (index-evict!))))

    (t/testing "no docs evicted yet"
      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (seq (->> (db/fetch-docs (:document-store *api*)
                                       (->> (db/entity-history index-snapshot picasso-id :desc {})
                                            (keep :content-hash)))
                        vals
                        (remove c/evicted-doc?))))))

    (binding [tx/*evict-all-on-legacy-time-ranges?* true]
      (let [{:keys [docs]} (index-evict!)]
        (db/submit-docs (:document-store *api*) docs)

        (t/testing "eviction removes docs"
          (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
            (t/is (empty? (->> (db/fetch-docs (:document-store *api*)
                                              (->> (db/entity-history index-snapshot picasso-id :desc {})
                                                   (keep :content-hash)))
                               vals
                               (remove c/evicted-doc?))))))))))

(t/deftest test-multiple-txs-in-same-ms-441
  (let [ivan {:crux.db/id :ivan}
        ivan1 (assoc ivan :value 1)
        ivan2 (assoc ivan :value 2)
        t #inst "2019-11-29"
        docs {(c/hash-doc ivan1) ivan1
              (c/hash-doc ivan2) ivan2}]
    (db/submit-docs (:document-store *api*) docs)

    (index-tx {::xt/tx-time t, ::xt/tx-id 1}
              [[:crux.tx/put :ivan (c/->id-buffer (c/hash-doc ivan1))]]
              docs)
    (index-tx {::xt/tx-time t, ::xt/tx-id 2}
              [[:crux.tx/put :ivan (c/->id-buffer (c/hash-doc ivan2))]]
              docs)

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/hash-doc ivan2))
                (c/->EntityTx (c/new-id :ivan) t t 1 (c/hash-doc ivan1))]
               (db/entity-history index-snapshot :ivan :desc {:with-corrections? true})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/hash-doc ivan2))]
               (db/entity-history index-snapshot :ivan :desc {:start-valid-time t
                                                              :start-tx {::xt/tx-id 2}})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 1 (c/hash-doc ivan1))]
               (db/entity-history index-snapshot :ivan :desc {:start-valid-time t
                                                              :start-tx {::xt/tx-id 1}})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/hash-doc ivan2))]
               (db/entity-history index-snapshot :ivan :asc {:start-valid-time t}))))))

(t/deftest test-entity-history-seq-corner-cases
  (let [ivan {:crux.db/id :ivan}
        ivan1 (assoc ivan :value 1)
        ivan2 (assoc ivan :value 2)
        t1 #inst "2020-05-01"
        t2 #inst "2020-05-02"
        docs {(c/hash-doc ivan1) ivan1
              (c/hash-doc ivan2) ivan2}]
    (db/submit-docs (:document-store *api*) docs)

    (index-tx {::xt/tx-time t1, ::xt/tx-id 1}
              [[:crux.tx/put :ivan (c/->id-buffer (c/hash-doc ivan1)) t1]]
              docs)
    (index-tx {::xt/tx-time t2, ::xt/tx-id 2}
              [[:crux.tx/put :ivan (c/->id-buffer (c/hash-doc ivan2)) t1]
               [:crux.tx/put :ivan (c/->id-buffer (c/hash-doc ivan2))]]
              docs)

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [etx-v1-t1 (c/->EntityTx (c/new-id :ivan) t1 t1 1 (c/hash-doc ivan1))
            etx-v1-t2 (c/->EntityTx (c/new-id :ivan) t1 t2 2 (c/hash-doc ivan2))
            etx-v2-t2 (c/->EntityTx (c/new-id :ivan) t2 t2 2 (c/hash-doc ivan2))]
        (letfn [(history-asc [opts]
                  (vec (db/entity-history index-snapshot :ivan :asc opts)))
                (history-desc [opts]
                  (vec (db/entity-history index-snapshot :ivan :desc opts)))]

          (t/testing "start is inclusive"
            (t/is (= [etx-v2-t2
                      etx-v1-t2]
                     (history-desc {:start-tx {::xt/tx-id 2} :start-valid-time t2})))

            (t/is (= [etx-v1-t2]
                     (history-desc {:start-valid-time t1})))

            (t/is (= [etx-v1-t2 etx-v2-t2]
                     (history-asc {:start-tx {::xt/tx-id 2}})))

            (t/is (= [etx-v1-t1 etx-v1-t2 etx-v2-t2]
                     (history-asc {:start-tx {::xt/tx-id 1} :start-valid-time t1
                                   :with-corrections? true}))))

          (t/testing "end is exclusive"
            (t/is (= [etx-v2-t2]
                     (history-desc {:start-valid-time t2, :start-tx {::xt/tx-id 2}
                                    :end-valid-time t1, :end-tx {::xt/tx-id 1}})))

            (t/is (= []
                     (history-desc {:end-valid-time t2})))

            (t/is (= [etx-v1-t1]
                     (history-asc {:end-tx {::xt/tx-id 2}})))

            (t/is (= []
                     (history-asc {:start-valid-time t1, :end-tx {::xt/tx-id 1}})))))))))

(t/deftest test-put-delete-range-semantics
  (t/are [txs history] (let [eid (keyword (gensym "ivan"))
                             ivan {:xt/id eid, :name "Ivan"}
                             res (mapv (fn [[value & vts]]
                                         (xt/submit-tx *api* [(into (if value
                                                                      [::xt/put (assoc ivan :value value)]
                                                                      [::xt/delete eid])
                                                                    vts)]))
                                       txs)
                             last-tx (last res)]

                         (xt/await-tx *api* last-tx nil)

                         (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
                           (t/is (= (for [[vt tx-idx value] history]
                                      [vt (get-in res [tx-idx ::xt/tx-id]) (c/hash-doc (when value
                                                                                         (assoc ivan :value value)))])

                                    (->> (db/entity-history index-snapshot eid :asc {})
                                         (map (juxt :vt :tx-id :content-hash)))))))

    ;; pairs
    ;; [[value vt ?end-vt] ...]
    ;; [[vt tx-idx value] ...]

    [[26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-26" 0 26]
     [#inst "2019-11-29" 0 nil]]

    ;; re-instates the previous value at the end of the range
    [[25 #inst "2019-11-25"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 1 26]
     [#inst "2019-11-29" 0 25]]

    ;; delete a range
    [[25 #inst "2019-11-25"]
     [nil #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 1 nil]
     [#inst "2019-11-29" 0 25]]

    ;; override a range
    [[25 #inst "2019-11-25" #inst "2019-11-27"]
     [nil #inst "2019-11-25" #inst "2019-11-27"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 1 nil]
     [#inst "2019-11-26" 2 26]
     [#inst "2019-11-27" 2 26]
     [#inst "2019-11-29" 0 nil]]

    ;; merge a range
    [[25 #inst "2019-11-25" #inst "2019-11-27"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 1 26]
     [#inst "2019-11-27" 1 26]
     [#inst "2019-11-29" 0 nil]]

    ;; shouldn't override the value at end-vt if there's one there
    [[25 #inst "2019-11-25"]
     [29 #inst "2019-11-29"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 2 26]
     [#inst "2019-11-29" 1 29]]

    ;; should re-instate 28 at the end of the range
    [[25 #inst "2019-11-25"]
     [28 #inst "2019-11-28"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 2 26]
     [#inst "2019-11-28" 2 26]
     [#inst "2019-11-29" 1 28]]

    ;; 26.1 should overwrite the full range
    [[28 #inst "2019-11-28"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]
     [26.1 #inst "2019-11-26"]]
    [[#inst "2019-11-26" 2 26.1]
     [#inst "2019-11-28" 2 26.1]
     [#inst "2019-11-29" 0 28]]

    ;; 27 should override the latter half of the range
    [[25 #inst "2019-11-25"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]
     [27 #inst "2019-11-27"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 1 26]
     [#inst "2019-11-27" 2 27]
     [#inst "2019-11-29" 0 25]]

    ;; 27 should still override the latter half of the range
    [[25 #inst "2019-11-25"]
     [28 #inst "2019-11-28"]
     [26 #inst "2019-11-26" #inst "2019-11-29"]
     [27 #inst "2019-11-27"]]
    [[#inst "2019-11-25" 0 25]
     [#inst "2019-11-26" 2 26]
     [#inst "2019-11-27" 3 27]
     [#inst "2019-11-28" 3 27]
     [#inst "2019-11-29" 1 28]]))

;; TODO: This test just shows that this is an issue, if we fix the
;; underlying issue this test should start failing. We can then change
;; the second assertion if we want to keep it around to ensure it
;; keeps working.
(t/deftest test-corrections-in-the-past-slowes-down-bitemp-144
  (let [ivan {:xt/id :ivan :name "Ivan"}
        start-valid-time #inst "2019"
        number-of-versions 1000
        tx (fix/submit+await-tx (for [n (range number-of-versions)]
                                  [::xt/put (assoc ivan :version n) (Date. (+ (.getTime start-valid-time) (inc (long n))))]))]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [baseline-time (let [start-time (System/nanoTime)
                                valid-time (Date. (+ (.getTime start-valid-time) number-of-versions))]
                            (t/testing "last version of entity is visible at now"
                              (t/is (= valid-time (:vt (db/entity-as-of index-snapshot :ivan valid-time (::xt/tx-id tx))))))
                            (- (System/nanoTime) start-time))]

        (let [start-time (System/nanoTime)
              valid-time (Date. (+ (.getTime start-valid-time) number-of-versions))]
          (t/testing "no version is visible before transactions"
            (t/is (nil? (db/entity-as-of index-snapshot :ivan valid-time -1)))
            (let [corrections-time (- (System/nanoTime) start-time)]
              ;; TODO: This can be a bit flaky. This assertion was
              ;; mainly there to prove the opposite, but it has been
              ;; fixed. Can be added back to sanity check when
              ;; changing indexes.
              #_(t/is (>= baseline-time corrections-time)))))))))

(t/deftest test-can-read-kv-tx-log
  (let [ivan {:xt/id :ivan :name "Ivan"}

        tx1-ivan (assoc ivan :version 1)
        tx1-valid-time #inst "2018-11-26"
        {tx1-id ::xt/tx-id
         tx1-tx-time ::xt/tx-time}
        (fix/submit+await-tx [[::xt/put tx1-ivan tx1-valid-time]])

        tx2-ivan (assoc ivan :version 2)
        tx2-petr {:xt/id :petr :name "Petr"}
        tx2-valid-time #inst "2018-11-27"
        {tx2-id ::xt/tx-id
         tx2-tx-time ::xt/tx-time}
        (fix/submit+await-tx [[::xt/put tx2-ivan tx2-valid-time]
                              [::xt/put tx2-petr tx2-valid-time]])]

    (with-open [log-iterator (db/open-tx-log (:tx-log *api*) nil)]
      (let [log (iterator-seq log-iterator)]
        (t/is (not (realized? log)))
        (t/is (= [{::xt/tx-id tx1-id
                   ::xt/tx-time tx1-tx-time
                   :xtdb.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc tx1-ivan) tx1-valid-time]]
                   ::xt/submit-tx-opts {}}
                  {::xt/tx-id tx2-id
                   ::xt/tx-time tx2-tx-time
                   :xtdb.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc tx2-ivan) tx2-valid-time]
                                             [:crux.tx/put (c/new-id :petr) (c/hash-doc tx2-petr) tx2-valid-time]]
                   ::xt/submit-tx-opts {}}]
                 log))))))

(t/deftest migrates-unhashed-tx-log-eids
  (let [doc {:crux.db/id :foo}
        doc-id (c/hash-doc doc)]
    (db/submit-docs (:document-store *api*) {doc-id doc})
    (doto @(db/submit-tx (:tx-log *api*) [[:crux.tx/match :foo (c/new-id c/nil-id-buffer)]
                                          [:crux.tx/put :foo doc-id]])
      (->> (xt/await-tx *api*)))

    (t/is (= {:xt/id :foo}
             (xt/entity (xt/db *api*) :foo)))

    (with-open [log-iterator (xt/open-tx-log *api* nil nil)]
      (let [evts (::xt/tx-events (first (iterator-seq log-iterator)))]
        ;; have to check not= too because Id's `equals` conforms its input to Id
        (t/is (not= [::xt/match :foo (c/new-id c/nil-id-buffer)]
                    (first evts)))
        (t/is (not= [::xt/put :foo doc-id]
                    (second evts)))
        (t/is (= [[::xt/match (c/new-id :foo) (c/new-id c/nil-id-buffer)]
                  [::xt/put (c/new-id :foo) doc-id]]
                 evts))))

    (fix/submit+await-tx [[::xt/delete :foo]])
    (t/is (nil? (xt/entity (xt/db *api*) :foo)))))

(t/deftest test-can-apply-transaction-fn
  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (let [v1-ivan {:xt/id :ivan :name "Ivan" :age 40}
          v4-ivan (assoc v1-ivan :name "IVAN")
          update-attribute-fn {:xt/id :update-attribute-fn
                               :xt/fn '(fn [ctx eid k f]
                                         [[::xt/put (update (xtdb.api/entity (xtdb.api/db ctx) eid) k (eval f))]])}]
      (fix/submit+await-tx [[::xt/put v1-ivan]
                            [::xt/put update-attribute-fn]])
      (t/is (= v1-ivan (xt/entity (xt/db *api*) :ivan)))
      (t/is (= update-attribute-fn (xt/entity (xt/db *api*) :update-attribute-fn)))
      (some-> (#'tx/reset-tx-fn-error) throw)

      (let [v2-ivan (assoc v1-ivan :age 41)]
        (fix/submit+await-tx [[::xt/fn :update-attribute-fn :ivan :age `inc]])
        (some-> (#'tx/reset-tx-fn-error) throw)
        (t/is (= v2-ivan (xt/entity (xt/db *api*) :ivan)))

        (t/testing "resulting documents are indexed"
          (t/is (= #{[41]} (xt/q (xt/db *api*)
                                 '[:find age :where [e :name "Ivan"] [e :age age]]))))

        (t/testing "exceptions"
          (t/testing "non existing tx fn"
            (let [tx (fix/submit+await-tx '[[::xt/fn :non-existing-fn]])]
              (t/is (= v2-ivan (xt/entity (xt/db *api*) :ivan)))
              (t/is (false? (xt/tx-committed? *api* tx)))))

          (t/testing "invalid arguments"
            (fix/submit+await-tx '[[::xt/fn :update-attribute-fn :ivan :age foo]])
            (t/is (thrown? clojure.lang.Compiler$CompilerException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "invalid results"
            (fix/submit+await-tx [[::xt/put
                                   {:xt/id :invalid-fn
                                    :xt/fn '(fn [ctx]
                                              [[::xt/foo]])}]])
            (fix/submit+await-tx '[[::xt/fn :invalid-fn]])
            (t/is (thrown-with-msg? IllegalArgumentException #"Invalid tx op" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "no ::xt/fn"
            (fix/submit+await-tx [[::xt/put
                                   {:xt/id :no-fn}]])

            (let [tx (fix/submit+await-tx '[[::xt/fn :no-fn]])]
              (t/is (false? (xt/tx-committed? *api* tx)))))

          (t/testing "not a fn"
            (fix/submit+await-tx [[::xt/put
                                   {:xt/id :not-a-fn
                                    :xt/fn 0}]])
            (fix/submit+await-tx '[[::xt/fn :not-a-fn]])
            (t/is (thrown? ClassCastException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "compilation errors"
            (fix/submit+await-tx [[::xt/put
                                   {:xt/id :compilation-error-fn
                                    :xt/fn '(fn [ctx]
                                              unknown-symbol)}]])
            (fix/submit+await-tx '[[::xt/fn :compilation-error-fn]])
            (t/is (thrown-with-msg? Exception #"Syntax error compiling" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "exception thrown"
            (fix/submit+await-tx [[::xt/put
                                   {:xt/id :exception-fn
                                    :xt/fn '(fn [ctx]
                                              (throw (RuntimeException. "foo")))}]])
            (fix/submit+await-tx '[[::xt/fn :exception-fn]])
            (t/is (thrown-with-msg? RuntimeException #"foo" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "still working after errors"
            (let [v3-ivan (assoc v1-ivan :age 40)]
              (fix/submit+await-tx [[::xt/fn :update-attribute-fn :ivan :age `dec]])
              (some-> (#'tx/reset-tx-fn-error) throw)
              (t/is (= v3-ivan (xt/entity (xt/db *api*) :ivan))))))

        (t/testing "sees in-transaction version of entities (including itself)"
          (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo 1}]])
          (let [tx (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo 2}]
                                         [::xt/put {:xt/id :doubling-fn
                                                    :xt/fn '(fn [ctx]
                                                              [[::xt/put (-> (xtdb.api/entity (xtdb.api/db ctx) :foo)
                                                                             (update :foo * 2))]])}]
                                         [::xt/fn :doubling-fn]])]

            (t/is (xt/tx-committed? *api* tx))
            (t/is (= {:xt/id :foo, :foo 4}
                     (xt/entity (xt/db *api*) :foo)))))

        (t/testing "function ops can return other function ops"
          (let [returns-fn {:xt/id :returns-fn
                            :xt/fn '(fn [ctx]
                                      [[::xt/fn :update-attribute-fn :ivan :name `string/upper-case]])}]
            (fix/submit+await-tx [[::xt/put returns-fn]])
            (fix/submit+await-tx [[::xt/fn :returns-fn]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= v4-ivan (xt/entity (xt/db *api*) :ivan)))))

        (t/testing "repeated 'merge' operation behaves correctly"
          (let [v5-ivan (merge v4-ivan
                               {:height 180
                                :hair-style "short"
                                :mass 60})
                merge-fn {:xt/id :merge-fn
                          :xt/fn `(fn [ctx# eid# m#]
                                    [[::xt/put (merge (xt/entity (xt/db ctx#) eid#) m#)]])}]
            (fix/submit+await-tx [[::xt/put merge-fn]])
            (fix/submit+await-tx [[::xt/fn :merge-fn :ivan {:mass 60, :hair-style "short"}]])
            (fix/submit+await-tx [[::xt/fn :merge-fn :ivan {:height 180}]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= v5-ivan (xt/entity (xt/db *api*) :ivan)))))

        (t/testing "function ops can return other function ops that also the forked ctx"
          (let [returns-fn {:xt/id :returns-fn
                            :xt/fn '(fn [ctx]
                                      [[::xt/put {:xt/id :ivan :name "modified ivan"}]
                                       [::xt/fn :update-attribute-fn :ivan :name `string/upper-case]])}]
            (fix/submit+await-tx [[::xt/put returns-fn]])
            (fix/submit+await-tx [[::xt/fn :returns-fn]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= {:xt/id :ivan :name "MODIFIED IVAN"}
                     (xt/entity (xt/db *api*) :ivan)))))

        (t/testing "can access current transaction on tx-fn context"
          (fix/submit+await-tx
           [[::xt/put
             {:xt/id :tx-metadata-fn
              :xt/fn `(fn [ctx#]
                        [[::xt/put {:xt/id :tx-metadata ::xt/current-tx (xt/indexing-tx ctx#)}]])}]])
          (let [submitted-tx (fix/submit+await-tx '[[::xt/fn :tx-metadata-fn]])]
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= {:xt/id :tx-metadata
                      ::xt/current-tx submitted-tx}
                     (xt/entity (xt/db *api*) :tx-metadata)))))))))

(t/deftest tx-fn-sees-in-tx-query-results
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo 1}]])
  (let [tx (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo 2}]
                                 [::xt/put {:xt/id :put
                                            :xt/fn '(fn [ctx doc]
                                                      [[::xt/put doc]])}]
                                 [::xt/fn :put {:xt/id :bar, :ref :foo}]
                                 [::xt/put {:xt/id :doubling-fn
                                            :xt/fn '(fn [ctx]
                                                      (let [db (xtdb.api/db ctx)]
                                                        [[::xt/put {:xt/id :prn-out
                                                                    :e (xtdb.api/entity db :bar)
                                                                    :q (first (xtdb.api/q db {:find '[e v]
                                                                                              :where '[[e :xt/id :bar]
                                                                                                       [e :ref v]]}))}]
                                                         [::xt/put (-> (xtdb.api/entity db :foo)
                                                                       (update :foo * 2))]]))}]
                                 [::xt/fn :doubling-fn]])]

    (t/is (xt/tx-committed? *api* tx))
    (t/is (= {:xt/id :foo, :foo 4}
             (xt/entity (xt/db *api*) :foo)))
    (t/is (= {:xt/id :prn-out
              :e {:xt/id :bar :ref :foo}
              :q [:bar :foo]}
             (xt/entity (xt/db *api*) :prn-out)))))

(t/deftest tx-log-evict-454 []
  (fix/submit+await-tx [[::xt/put {:xt/id :to-evict}]])
  (fix/submit+await-tx [[::xt/cas {:xt/id :to-evict} {:xt/id :to-evict :test "test"}]])
  (fix/submit+await-tx [[::xt/evict :to-evict]])

  (with-open [log-iterator (xt/open-tx-log *api* nil true)]
    (t/is (= [[[::xt/put
                {:xt/id #xtdb/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                 ::xt/evicted? true}]]
              [[::xt/cas
                {:xt/id #xtdb/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                 ::xt/evicted? true}
                {:xt/id #xtdb/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                 ::xt/evicted? true}]]
              [[::xt/evict
                #xtdb/id "6abe906510aa2263737167c12c252245bdcf6fb0"]]]
             (->> (iterator-seq log-iterator)
                  (map ::xt/tx-ops))))))

(t/deftest transaction-fn-return-values-457
  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (let [nil-fn {:xt/id :nil-fn
                  :xt/fn '(fn [ctx] nil)}
          false-fn {:xt/id :false-fn
                    :xt/fn '(fn [ctx] false)}]

      (fix/submit+await-tx [[::xt/put nil-fn]
                            [::xt/put false-fn]])
      (fix/submit+await-tx [[::xt/fn :nil-fn]
                            [::xt/put {:xt/id :foo
                                       :foo? true}]])

      (t/is (= {:xt/id :foo, :foo? true}
               (xt/entity (xt/db *api*) :foo)))

      (fix/submit+await-tx [[::xt/fn :false-fn]
                            [::xt/put {:xt/id :bar
                                       :bar? true}]])

      (t/is (nil? (xt/entity (xt/db *api*) :bar))))))

(t/deftest map-ordering-362
  (t/testing "cas is independent of map ordering"
    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo :bar}]])
    (fix/submit+await-tx [[::xt/cas {:foo :bar, :xt/id :foo} {:xt/id :foo, :foo :baz}]])

    (t/is (= {:xt/id :foo, :foo :baz}
             (xt/entity (xt/db *api*) :foo))))

  (t/testing "entities with map keys can be retrieved regardless of ordering"
    (let [doc {:xt/id {:foo 1, :bar 2}}]
      (fix/submit+await-tx [[::xt/put doc]])

      (t/is (= doc (xt/entity (xt/db *api*) {:foo 1, :bar 2})))
      (t/is (= doc (xt/entity (xt/db *api*) {:bar 2, :foo 1})))))

  (t/testing "entities with map values can be joined regardless of ordering"
    (let [doc {:xt/id {:foo 2, :bar 4}}]
      (fix/submit+await-tx [[::xt/put doc]
                            [::xt/put {:xt/id :baz, :joins {:bar 4, :foo 2}}]
                            [::xt/put {:xt/id :quux, :joins {:foo 2, :bar 4}}]])

      (t/is (= #{[{:foo 2, :bar 4} :baz]
                 [{:foo 2, :bar 4} :quux]}
               (xt/q (xt/db *api*) '{:find [parent child]
                                     :where [[parent :xt/id _]
                                             [child :joins parent]]}))))))

(t/deftest incomparable-colls-1001
  (t/testing "can store and retrieve incomparable colls (#1001)"
    (let [foo {:xt/id :foo
               :foo {:bar #{7 "hello"}}}]
      (fix/submit+await-tx [[::xt/put foo]])

      (t/is (= #{[foo]}
               (xt/q (xt/db *api*) '{:find [(pull ?e [*])]
                                     :where [[?e :foo {:bar #{"hello" 7}}]]}))))

    (let [foo {:xt/id :foo
               :foo {{:foo 1} :foo1
                     {:foo 2} :foo2}}]
      (fix/submit+await-tx [[::xt/put foo]])

      (t/is (= #{[foo]}
               (xt/q (xt/db *api*) '{:find [(pull ?e [*])]
                                     :where [[?e :foo {{:foo 2} :foo2, {:foo 1} :foo1}]]}))))))

(t/deftest test-java-ids-and-values-1398
  (letfn [(test-id [id]
            (t/testing "As ID"
              (with-open [node (xt/start-node {})]
                (let [doc {:xt/id id}]
                  (fix/submit+await-tx node [[::xt/put doc]])
                  (t/is (= doc (xt/entity (xt/db node) id)))
                  (t/is #{[id]} (xt/q (xt/db node) '{:find [?id]
                                                     :where [[?id :xt/id]]}))))))
          (test-value [value]
            (t/testing "As Value"
              (with-open [node (xt/start-node {})]
                (let [doc {:xt/id :foo :bar value}]
                  (fix/submit+await-tx node [[::xt/put doc]])
                  (t/is (= doc (xt/entity (xt/db node) :foo)))
                  (t/is #{[value]} (xt/q (xt/db node) '{:find [?val]
                                                        :where [[?id :bar ?val]]}))))))]

    (t/testing "Keyword"
      (doto (Keyword/intern "foo")
        (test-id) (test-value)))

    (t/testing "String"
      (doto "foo"
        (test-id) (test-value)))

    (t/testing "Long"
      (doto 100
        (test-id) (test-value)))

    (t/testing "UUID"
      (doto (UUID/randomUUID)
        (test-id) (test-value)))

    (t/testing "URI"
      (doto (URI/create "mailto:hello@xtdb.com")
        (test-id) (test-value)))

    (t/testing "URL"
      (doto (URL. "https://github.com/xtdb/xtdb")
        (test-id) (test-value)))

    (t/testing "IPersistentMap"
      (doto (-> (PersistentArrayMap/EMPTY)
                (.assoc "foo" "bar"))
        (test-id) (test-value)))

    (t/testing "Set"
      (doto (HashSet.)
        (.add "foo")
        (.add "bar")
        (test-value)))

    (t/testing "Singleton Map"
      (doto (Collections/singletonMap "foo" "bar")
        (test-id) (test-value)))

    (t/testing "HashMap with single entry"
      (doto (HashMap.)
        (.put "foo" "bar")
        (test-id) (test-value)))

    (t/testing "HashMap with multiple entries"
      (doto (HashMap.)
        (.put "foo" "bar")
        (.put "baz" "waka")
        (test-id) (test-value)))

    (t/testing "HashMap with entries added in different order"
      (let [val1 (doto (HashMap.)
                   (.put "foo" "bar")
                   (.put "baz" "waka"))
            val2 (doto (HashMap.)
                   (.put "baz" "waka")
                   (.put "foo" "bar"))]

        (t/is (= val1 val2))
        (t/testing "As ID"
          (with-open [node (xt/start-node {})]
            (let [doc {:xt/id val1}]
              (fix/submit+await-tx node [[::xt/put doc]])
              (t/is (= doc (xt/entity (xt/db node) val2)))
              (let [result (xt/q (xt/db node) '{:find [?id]
                                                :where [[?id :xt/id]]})]
                (t/is #{[val1]} result)
                (t/is #{[val2]} result)))))

        (t/testing "As Value"
          (with-open [node (xt/start-node {})]
            (let [doc {:xt/id :foo :bar val1}]
              (fix/submit+await-tx node [[::xt/put doc]])
              (t/is (= doc (xt/entity (xt/db node) :foo)))
              (let [result (xt/q (xt/db node) '{:find [?val]
                                                :where [[?id :bar ?val]]})]
                (t/is #{[val1]} result)
                (t/is #{[val2]} result)))))))))

(t/deftest overlapping-valid-time-ranges-434
  (let [_ (fix/submit+await-tx
           [[::xt/put {:xt/id :foo, :v 10} #inst "2019-01-01"]
            [::xt/put {:xt/id :foo, :v 10} #inst "2020-01-10"]
            [::xt/put {:xt/id :bar, :v 5} #inst "2020-01-05"]
            [::xt/put {:xt/id :bar, :v 10} #inst "2020-01-10"]

            [::xt/put {:xt/id :baz, :v 10} #inst "2020-01-10"]])

        last-tx (fix/submit+await-tx [[::xt/put {:xt/id :foo, :v 10} #inst "2019-01-01"]
                                      [::xt/put {:xt/id :bar, :v 7} #inst "2020-01-07"]
                                      ;; mixing foo and bar shouldn't matter
                                      [::xt/put {:xt/id :foo, :v 8} #inst "2020-01-08" #inst "2020-01-12"] ; reverts to 10 afterwards
                                      [::xt/put {:xt/id :foo, :v 9} #inst "2020-01-09" #inst "2020-01-11"] ; reverts to 8 afterwards, then 10
                                      [::xt/put {:xt/id :bar, :v 8} #inst "2020-01-08" #inst "2020-01-09"] ; reverts to 7 afterwards
                                      [::xt/put {:xt/id :bar, :v 11} #inst "2020-01-11" #inst "2020-01-12"] ; reverts to 10 afterwards
                                      ])

        db (xt/db *api*)]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [eid->history (fn [eid]
                           (let [history (db/entity-history index-snapshot
                                                            (c/new-id eid) :asc
                                                            {:start-valid-time #inst "2020-01-01"})
                                 docs (db/fetch-docs (:document-store *api*) (map :content-hash history))]
                             (->> history
                                  (mapv (fn [{:keys [content-hash vt]}]
                                          [vt (:v (get docs content-hash))])))))]
        ;; transaction functions, asserts both still apply at the start of the transaction
        (t/is (= [[#inst "2020-01-08" 8]
                  [#inst "2020-01-09" 9]
                  [#inst "2020-01-10" 9]
                  [#inst "2020-01-11" 8]
                  [#inst "2020-01-12" 10]]
                 (eid->history :foo)))

        (t/is (= [[#inst "2020-01-05" 5]
                  [#inst "2020-01-07" 7]
                  [#inst "2020-01-08" 8]
                  [#inst "2020-01-09" 7]
                  [#inst "2020-01-10" 10]
                  [#inst "2020-01-11" 11]
                  [#inst "2020-01-12" 10]]
                 (eid->history :bar)))))))

(t/deftest cas-docs-not-evicting-371
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo :bar}]
                        [::xt/put {:xt/id :frob :foo :bar}]])

  (fix/submit+await-tx [[::xt/cas {:xt/id :foo, :foo :baz} {:xt/id :foo, :foo :quux}]
                        [::xt/put {:xt/id :frob :foo :baz}]])
  (fix/submit+await-tx [[::xt/evict :foo]])

  (let [docs (db/fetch-docs (:document-store *api*)
                            #{(c/hash-doc {:xt/id :foo, :foo :bar})
                              (c/hash-doc {:xt/id :foo, :foo :baz})
                              (c/hash-doc {:xt/id :foo, :foo :quux})})]

    (t/is (= 3 (count docs)))
    (t/is (every? (comp c/evicted-doc? val) docs)))

  (t/testing "even though the CaS was unrelated, the whole transaction fails - we should still evict those docs"
    (fix/submit+await-tx [[::xt/evict :frob]])
    (let [docs (db/fetch-docs (:document-store *api*)
                              #{(c/hash-doc {:xt/id :frob, :foo :bar})
                                (c/hash-doc {:xt/id :frob, :foo :baz})})]
      (t/is (= 2 (count docs)))
      (t/is (every? (comp c/evicted-doc? val) docs)))))

(t/deftest raises-tx-events-422
  (let [!events (atom [])
        !latch (promise)]
    (bus/listen (:bus *api*) {::xt/event-types #{::tx/indexing-tx ::tx/indexed-tx}}
                (fn [evt]
                  (swap! !events conj evt)
                  (when (= ::tx/indexed-tx (::xt/event-type evt))
                    (deliver !latch @!events))))

    (let [doc-1 {:xt/id :foo, :value 1}
          doc-2 {:xt/id :bar, :value 2}
          doc-ids #{(c/hash-doc doc-1) (c/hash-doc doc-2)}
          submitted-tx (fix/submit+await-tx [[::xt/put doc-1] [::xt/put doc-2]])]

      (when (= ::timeout (deref !latch 500 ::timeout))
        (t/is false))

      (t/is (= [{::xt/event-type ::tx/indexing-tx, :submitted-tx submitted-tx}
                {::xt/event-type ::tx/indexed-tx,
                 :submitted-tx submitted-tx,
                 :committed? true
                 :doc-ids doc-ids
                 :av-count 4
                 ::txe/tx-events [[:crux.tx/put
                                   #xtdb/id "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"
                                   #xtdb/id "974e28e6484fb6c66e5ca6444ec616207800d815"]
                                  [:crux.tx/put
                                   #xtdb/id "62cdb7020ff920e5aa642c3d4066950dd1f01f4d"
                                   #xtdb/id "f2cb628efd5123743c30137b08282b9dee82104a"]]}]
               (-> (vec @!events)
                   (update 1 dissoc :bytes-indexed)))))))

(t/deftest await-fails-quickly-738
  (with-redefs [tx/index-tx-event (fn [_ _ _]
                                    (Thread/sleep 100)
                                    (throw (ex-info "test error for await-fails-quickly-738" {})))]
    (let [last-tx (xt/submit-tx *api* [[::xt/put {:xt/id :foo}]])]
      (t/testing "Testing fail while we are awaiting an event"
        (t/is (thrown-with-msg?
               Exception
               #"Transaction ingester aborted"
               (xt/await-tx *api* last-tx (Duration/ofMillis 1000)))))
      (t/testing "Testing fail before we await an event"
        (t/is (thrown-with-msg?
               Exception
               #"Transaction ingester aborted"
               (xt/await-tx *api* last-tx (Duration/ofMillis 1000))))))))

(t/deftest test-evict-documents-with-common-attributes
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :count 1}]
                        [::xt/put {:xt/id :bar, :count 1}]])

  (fix/submit+await-tx [[::xt/evict :foo]])

  (t/is (= #{[:bar]}
           (xt/q (xt/db *api*) '{:find [?e]
                                 :where [[?e :count 1]]}))))

(t/deftest replaces-tx-fn-arg-docs
  (fix/submit+await-tx [[::xt/put {:xt/id :put-ivan
                                   :xt/fn '(fn [ctx doc]
                                             [[::xt/put (assoc doc :xt/id :ivan)]])}]])
  (t/testing "replaces args doc with resulting ops"
    (fix/submit+await-tx [[::xt/fn :put-ivan {:name "Ivan"}]])

    (t/is (= {:xt/id :ivan, :name "Ivan"}
             (xt/entity (xt/db *api*) :ivan)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan, :name "Ivan"})]]}
               (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                   (get arg-doc-id)
                   (c/crux->xt)
                   (dissoc :xt/id))))))

  (t/testing "replaces fn with no args"
    (fix/submit+await-tx [[::xt/put {:xt/id :no-args
                                     :xt/fn '(fn [ctx]
                                               [[::xt/put {:xt/id :no-fn-args-doc}]])}]])
    (fix/submit+await-tx [[::xt/fn :no-args]])

    (t/is (= {:xt/id :no-fn-args-doc}
             (xt/entity (xt/db *api*) :no-fn-args-doc)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :no-fn-args-doc) (c/hash-doc {:xt/id :no-fn-args-doc})]]}
               (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                   (get arg-doc-id)
                   (c/crux->xt)
                   (dissoc :xt/id))))))

  (t/testing "nested tx-fn"
    (fix/submit+await-tx [[::xt/put {:xt/id :put-bob-and-ivan
                                     :xt/fn '(fn [ctx bob ivan]
                                               [[::xt/put (assoc bob :xt/id :bob)]
                                                [::xt/fn :put-ivan ivan]])}]])

    (fix/submit+await-tx [[::xt/fn :put-bob-and-ivan {:name "Bob"} {:name "Ivan2"}]])

    (t/is (= {:xt/id :ivan, :name "Ivan2"}
             (xt/entity (xt/db *api*) :ivan)))

    (t/is (= {:xt/id :bob, :name "Bob"}
             (xt/entity (xt/db *api*) :bob)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) 1)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))
          arg-doc (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                      (get arg-doc-id)
                      (c/crux->xt))

          sub-arg-doc-id (-> arg-doc :crux.db.fn/tx-events second last)
          sub-arg-doc (-> (db/fetch-docs (:document-store *api*) #{sub-arg-doc-id})
                          (get sub-arg-doc-id)
                          (c/crux->xt))]

      (t/is (= {:xt/id (:xt/id arg-doc)
                :crux.db.fn/tx-events [[:crux.tx/put (c/new-id :bob) (c/hash-doc {:xt/id :bob, :name "Bob"})]
                                       [:crux.tx/fn (c/new-id :put-ivan) sub-arg-doc-id]]}
               arg-doc))

      (t/is (= {:xt/id (:xt/id sub-arg-doc)
                :crux.db.fn/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan2"})]]}
               sub-arg-doc))))

  (t/testing "copes with args doc having been replaced"
    (let [sergei {:xt/id :sergei
                  :name "Sergei"}
          arg-doc {:xt/id :args
                   :crux.db.fn/tx-events [[:crux.tx/put :sergei (c/hash-doc sergei)]]}]
      (db/submit-docs (:document-store *api*)
                      {(c/hash-doc arg-doc) arg-doc
                       (c/hash-doc sergei) sergei})
      (let [tx @(db/submit-tx (:tx-log *api*) [[:crux.tx/fn :put-sergei (c/hash-doc arg-doc)]])]
        (xt/await-tx *api* tx)

        (t/is (= sergei (xt/entity (xt/db *api*) :sergei))))))

  (t/testing "failed tx-fn"
    (fix/submit+await-tx [[::xt/fn :put-petr {:name "Petr"}]])

    (t/is (nil? (xt/entity (xt/db *api*) :petr)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (true? (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                       (get arg-doc-id)
                       :crux.db.fn/failed?))))))

(t/deftest handles-legacy-tx-fns-with-no-args-doc
  ;; we used to not submit args docs if the tx-fn was 0-arg, and hence had nothing to replace
  ;; we don't want the fix to assume that all tx-fns on the tx-log have an arg-doc
  (let [tx-fn (-> {:xt/id :tx-fn,
                   :xt/fn '(fn [ctx]
                             [[::xt/put {:xt/id :ivan}]])}
                  (c/xt->crux))
        docs {(c/hash-doc tx-fn) tx-fn}]
    (db/submit-docs (:document-store *api*) docs)

    (index-tx {::xt/tx-time (Date.), ::xt/tx-id 0}
              [[:crux.tx/put (c/new-id :tx-fn) tx-fn]]
              docs)

    (index-tx {::xt/tx-time (Date.), ::xt/tx-id 1}
              [[:crux.tx/fn :tx-fn]]
              docs)

    (t/is (= {:xt/id :ivan}
             (xt/entity (xt/db *api*) :ivan)))))

(t/deftest test-tx-fn-doc-race-1049
  (let [put-fn {:xt/id :put-fn
                :xt/fn '(fn [ctx doc]
                          [[::xt/put doc]])}
        foo-doc {:crux.db/id :foo}
        !arg-doc-resps (atom [{:crux.db.fn/args [foo-doc]}
                              {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :foo) (c/hash-doc foo-doc)]]}])
        ->mocked-doc-store (fn [_opts]
                             (reify db/DocumentStore
                               (submit-docs [_ docs])
                               (fetch-docs [_ ids]
                                 (->> ids
                                      (into {} (map (juxt identity
                                                          (some-fn {(c/hash-doc put-fn) (c/xt->crux put-fn)
                                                                    (c/hash-doc foo-doc) foo-doc}
                                                                   (fn [id]
                                                                     (let [[[doc] _] (swap-vals! !arg-doc-resps rest)]
                                                                       (merge doc {:crux.db/id id})))))))))))]
    (with-open [node (xt/start-node {:xtdb/document-store ->mocked-doc-store})]
      (xt/submit-tx node [[::xt/put put-fn]])
      (xt/submit-tx node [[::xt/fn :put-fn foo-doc]])
      (xt/sync node)
      (t/is (= #{[:put-fn] [:foo]}
               (xt/q (xt/db node) '{:find [?e]
                                    :where [[?e :xt/id]]}))))))

(t/deftest ensure-tx-fn-arg-docs-replaced-even-when-tx-aborts-960
  (fix/submit+await-tx [[::xt/put {:xt/id :foo
                                   :xt/fn '(fn [ctx arg] [])}]
                        [::xt/match :foo {:xt/id :foo}]
                        [::xt/fn :foo :foo-arg]])

  (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                     (-> (iterator-seq tx-log)
                         first
                         :xtdb.tx.event/tx-events
                         last
                         last))
        arg-doc (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                    (get arg-doc-id)
                    (c/crux->xt))]
    (t/is (= {:crux.db.fn/failed? true}
             (dissoc arg-doc :xt/id)))
    (t/is (uuid? (:xt/id arg-doc)))))

(t/deftest test-documents-with-int-short-byte-float-eids-1043
  (fix/submit+await-tx [[::xt/put {:xt/id (int 10), :name "foo"}]
                        [::xt/put {:xt/id (short 12), :name "bar"}]
                        [::xt/put {:xt/id (byte 16), :name "baz"}]
                        [::xt/put {:xt/id (float 1.1), :name "quux"}]])

  (let [db (xt/db *api*)]
    (t/is (= #{["foo"] ["bar"] ["baz"] ["quux"]}
             (xt/q (xt/db *api*)
                   '{:find [?name]
                     :where [[?e :name ?name]]})))

    (t/is (= {:xt/id (long 10), :name "foo"}
             (xt/entity db (int 10))))

    (t/is (= {:xt/id (long 10), :name "foo"}
             (xt/entity db (long 10))))

    (t/is (= #{[10 "foo"]}
             (xt/q (xt/db *api*)
                   {:find '[?e ?name]
                    :where '[[?e :name ?name]]
                    :args [{:?e (int 10)}]}))))

  (t/testing "10 as int and long are the same key"
    (fix/submit+await-tx [[::xt/put {:xt/id 10, :name "foo2"}]])

    (t/is (= #{[10 "foo2"]}
             (xt/q (xt/db *api*)
                   {:find '[?e ?name]
                    :where '[[?e :name ?name]]
                    :args [{:?e (int 10)}]})))))

(t/deftest test-put-evict-in-same-transaction-1337
  (t/testing "put then evict"
    (fix/submit+await-tx [[::xt/put {:xt/id :test1/a, :test1? true}]])
    (fix/submit+await-tx [[::xt/put {:xt/id :test1/b, :test1? true, :test1/evicted? true}]
                          [::xt/evict :test1/b]])
    (let [db (xt/db *api*)]
      (t/is (= {:xt/id :test1/a, :test1? true} (xt/entity db :test1/a)))
      (t/is (nil? (xt/entity db :test1/b)))

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (empty? (db/av index-snapshot :test1/evicted? nil)))
        (t/is (empty? (db/entity-history index-snapshot :test1/b :asc {}))))

      (t/is (= #{[:test1/a]} (xt/q db '{:find [?e], :where [[?e :test1? true]]})))
      (t/is (= #{} (xt/q db '{:find [?e], :where [[?e :test1/evicted? true]]})))))

  (t/testing "put then evict an earlier entity"
    (fix/submit+await-tx [[::xt/put {:xt/id :test2/a, :test2? true, :test2/evicted? true}]])
    (fix/submit+await-tx [[::xt/put {:xt/id :test2/b, :test2? true}]
                          [::xt/evict :test2/a]])
    (let [db (xt/db *api*)]
      (t/is (nil? (xt/entity db :test2/a)))
      (t/is (= {:xt/id :test2/b, :test2? true} (xt/entity db :test2/b)))

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (empty? (db/av index-snapshot :test2/evicted? nil)))
        (t/is (empty? (db/entity-history index-snapshot :test2/a :asc {}))))

      (t/is (= #{[:test2/b]} (xt/q db '{:find [?e], :where [[?e :test2? true]]})))
      (t/is (= #{} (xt/q db '{:find [?e], :where [[?e :test2/evicted? true]]})))))

  (t/testing "evict then put"
    (fix/submit+await-tx [[::xt/put {:xt/id :test3/a, :test3? true}]])

    (fix/submit+await-tx [[::xt/evict :test3/a]
                          [::xt/put {:xt/id :test3/b, :test3? true}]])
    (let [db (xt/db *api*)]
      (t/is (nil? (xt/entity db :test3/a)))
      (t/is (= {:xt/id :test3/b, :test3? true} (xt/entity db :test3/b)))
      (t/is (= #{[:test3/b]} (xt/q db '{:find [?e], :where [[?e :test3? true]]})))))

  ;; TODO fails, see #1337
  #_
  (t/testing "evict then re-put"
    (fix/submit+await-tx [[::xt/put {:xt/id :test4, :test4? true}]])

    (fix/submit+await-tx [[::xt/evict :test4]
                          [::xt/put {:xt/id :test4, :test4? true}]])
    (let [db (xt/db *api*)]
      (t/is (= {:xt/id :test4, :test4? true} (xt/entity db :test4)))
      (t/is (= #{[:test4]} (xt/q db '{:find [?e], :where [[?e :test4? true]]}))))))


(t/deftest test-avs-only-shared-by-evicted-entities-1338
  (t/testing "only one entity, AV removed"
    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :evict-me? true}]])
    (fix/submit+await-tx [[::xt/evict :foo]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (empty? (db/av index-snapshot :evict-me? nil)))))

  ;; TODO fails, see #1338
  #_
  (t/testing "shared by multiple evicted entities"
    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :evict-me? true}]
                          [::xt/put {:xt/id :bar, :evict-me? true}]])
    (fix/submit+await-tx [[::xt/evict :foo]
                          [::xt/evict :bar]])
    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (empty? (db/av index-snapshot :evict-me? nil))))))

(t/deftest node-shutdown-interrupts-tx-ingestion
  (let [op-count 10
        !calls (atom 0)
        node (xt/start-node {})]
    (with-redefs [tx/index-tx-event (let [f tx/index-tx-event]
                                      (fn [& args]
                                        (let [target-time (+ (System/currentTimeMillis) 200)]
                                          (while (< (System/currentTimeMillis) target-time)))
                                        (swap! !calls inc)
                                        (apply f args)))]
      @(try
         (let [tx (xt/submit-tx node (repeat op-count [::xt/put {:xt/id :foo}]))
               await-fut (future
                           (t/is (thrown? InterruptedException (xt/await-tx node tx))))]
           (Thread/sleep 100) ; to ensure the await starts before the node closes
           await-fut)
         (finally
           (.close node))))
    (t/is (instance? InterruptedException (db/ingester-error (:tx-ingester node))))
    (t/is (< @!calls op-count))))

(t/deftest empty-tx-can-be-awaited-1519
  (let [tx (xt/submit-tx *api* [])
        _ (xt/await-tx *api* tx (Duration/ofSeconds 1))]
    (t/is (= (select-keys tx [::xt/tx-id]) (xt/latest-submitted-tx *api*)))
    (t/is (= tx (xt/latest-completed-tx *api*)))
    (t/is (xt/tx-committed? *api* tx))))

(t/deftest handles-secondary-indices
  (letfn [(with-persistent-golden-stores [node-config db-dir]
            (-> node-config
                (assoc :xtdb/tx-log {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                :db-dir (io/file db-dir "txs")}}
                       :xtdb/document-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                        :db-dir (io/file db-dir "docs")}})))

          (with-persistent-indices [node-config idx-dir]
            (-> node-config
                (assoc :xtdb/index-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                     :db-dir idx-dir}})))

          (with-secondary-index
            ([node-config after-tx-id opts process-tx-f]
             (-> node-config
                 (assoc ::index2 (-> (fn [{:keys [secondary-indices]}]
                                       (tx/register-index! secondary-indices after-tx-id opts process-tx-f)
                                       nil)
                                     (with-meta {::sys/deps {:secondary-indices :xtdb/secondary-indices}
                                                 ::sys/before #{[:xtdb/tx-ingester]}}))))))]

    (t/testing "indexes into secondary indices"
      (let [!txs (atom [])]
        (with-open [node (xt/start-node (-> {}
                                            (with-secondary-index nil {}
                                              (fn [tx]
                                                (swap! !txs conj tx)))))]
          (fix/submit+await-tx node [[::xt/put {:xt/id :foo}]])
          (t/is (= [0] (map ::xt/tx-id @!txs))))))

    (t/testing "secondary indices catch up to XTDB indices on node startup"
      (fix/with-tmp-dirs #{db-dir idx-dir}
        (with-open [node (xt/start-node (-> {}
                                            (with-persistent-golden-stores db-dir)
                                            (with-persistent-indices idx-dir)))]
          (fix/submit+await-tx node [[::xt/put {:xt/id :foo}]]))

        (let [!txs (atom [])]
          (with-open [_node (xt/start-node (-> {}
                                               (with-persistent-golden-stores db-dir)
                                               (with-persistent-indices idx-dir)
                                               (with-secondary-index nil {}
                                                 (fn [tx]
                                                   (swap! !txs conj tx)))))]
            ;; NOTE: don't need `sync` - should happen before node becomes available.
            (t/is (= [0] (map ::xt/tx-id @!txs)))))))

    (t/testing "XTDB catches up without replaying tx to secondary indices"
      (fix/with-tmp-dirs #{db-dir}
        (let [!txs (atom [])]
          (with-open [node (xt/start-node (-> {}
                                              (with-persistent-golden-stores db-dir)
                                              (with-secondary-index nil {}
                                                (fn [tx]
                                                  (swap! !txs conj tx)))))]
            (fix/submit+await-tx node [[::xt/put {:xt/id :foo}]]))

          (with-open [node (xt/start-node (-> {}
                                              (with-persistent-golden-stores db-dir)
                                              (with-secondary-index 0 {}
                                                (fn [tx]
                                                  (swap! !txs conj tx)))))]
            (xt/sync node)
            (t/is (= 0 (::xt/tx-id (xt/latest-completed-tx node))))
            (t/is (= [0] (map ::xt/tx-id @!txs)))))))

    (t/testing "passes through tx-ops on request"
      (fix/with-tmp-dirs #{db-dir idx-dir}
        (let [!txs (atom [])]
          (with-open [node (xt/start-node (-> {}
                                              (with-persistent-golden-stores db-dir)
                                              (with-persistent-indices idx-dir)
                                              (with-secondary-index nil {:with-tx-ops? true}
                                                (fn [tx]
                                                  (swap! !txs conj tx)))))]

            (fix/submit+await-tx node [[::xt/put {:xt/id :ivan :name "Ivan"}]])

            (t/is (= [{::xt/tx-id 0
                       ::xt/tx-ops [[::xt/put {:xt/id :ivan, :name "Ivan"}]]}]
                     (->> @!txs
                          (map #(select-keys % [::xt/tx-id ::xt/tx-ops])))))))))

    (with-redefs [log-impl/enabled? (constantly false)]
      (t/testing "handles secondary indexes blowing up"
        (with-open [node (xt/start-node (-> {}
                                            (with-secondary-index nil {}
                                              (fn [_tx]
                                                (throw (ex-info "boom!" {}))))))]

          (t/is (thrown-with-msg? ExceptionInfo #"boom!"
                                  (try
                                    (fix/submit+await-tx node [[::xt/put {:xt/id :foo}]])
                                    (catch Exception e
                                      (throw (.getCause e))))))
          (t/is (nil? (xt/latest-completed-tx node)))))

      (t/testing "handles secondary indexes blowing up on startup"
        (fix/with-tmp-dirs #{db-dir idx-dir}
          (with-open [node (xt/start-node (-> {}
                                              (with-persistent-golden-stores db-dir)
                                              (with-persistent-indices idx-dir)))]
            (fix/submit+await-tx node [[::xt/put {:xt/id :foo}]]))

          (t/is (thrown-with-msg? ExceptionInfo #"boom!"
                                  (try
                                    (xt/start-node (-> {}
                                                       (with-persistent-golden-stores db-dir)
                                                       (with-persistent-indices idx-dir)
                                                       (with-secondary-index nil {}
                                                         (fn [_tx]
                                                           (throw (ex-info "boom!" {}))))))
                                    (catch Exception e
                                      (throw (.getCause e)))))))))))

(t/deftest tx-committed-throws-correctly-1579
  (let [tx (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])]
    (t/is (xt/tx-committed? *api* tx))
    (t/is (xt/tx-committed? *api* {::xt/tx-id 0}))

    (t/is (thrown? xtdb.IllegalArgumentException (xt/tx-committed? *api* nil)))
    (t/is (thrown? xtdb.IllegalArgumentException (xt/tx-committed? *api* {})))
    (t/is (thrown? xtdb.IllegalArgumentException (xt/tx-committed? *api* {::xt/tx-id nil})))
    (t/is (thrown? ClassCastException (xt/tx-committed? *api* {::xt/tx-id :a})))

    ;; "skipped" tx-ids (e.g. handling Kafka offsets), and negative or non-integer tx-ids are also incorrect but not currently detected
    #_
    (t/is (thrown? xtdb.api.NodeOutOfSyncException (xt/tx-committed? *api* {::xt/tx-id -1.5})))))

(t/deftest can-query-documents-with-url-id-1638
  ;; TODO: the (XT) hash of the URL depends on the serialized form of the URL object,
  ;; which in turn depends on its cached hashCode field. couple of fun things here:
  ;; - if nothing has yet computed the hashCode, it serializes `-1`
  ;; - the hashCode computation depends on the resolved IP address of the domain name (non-deterministic)
  ;; we (arguably incorrectly) expect the serialized form to be deterministic
  ;; we could consider re-implementing c/value->buffer for URL, but this would have backwards compatibility implications

  #_ ;
  (let [url (URL. "https://xtdb.com")
        doc {:xt/id url}]
    (fix/submit+await-tx [[::xt/put doc]])

    (let [db (xt/db *api*)]
      (t/is (= doc (xt/entity db url)))

      (t/is (= #{[url]}
               (xt/q db '{:find [id]
                          :where [[id :xt/id]]}))))))

(t/deftest match-failure-of-a-put-shouldnt-affect-subsequent-indexing-1683
  (fix/submit+await-tx [[::xt/match :bar {:xt/id :bar}]
                        [::xt/put {:xt/id :foo}]])

  (let [db (xt/db *api*)]
    (t/is (nil? (xt/entity db :foo))))

  (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])

  (let [db (xt/db *api*)]
    (t/is (= {:xt/id :foo}
             (xt/entity db :foo)))

    (t/is (= #{[:foo]}
             (xt/q db '{:find [e], :where [[e :xt/id]]})))))

;; https://github.com/xtdb/xtdb/pull/1808 (try to catch batching regression)
(t/deftest mix-sized-transaction-test
  (fix/with-tmp-dirs #{db-dir}
    (let [ctr (atom 0)
          cfg {:xtdb/tx-log {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                        :db-dir (io/file db-dir "txs")}}
               :xtdb/document-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                                                :db-dir (io/file db-dir "docs")}}}]
      (with-open [node (xt/start-node cfg)]
        (doseq [tx-doc-count (concat
                               (range 1 10)
                               (range 1 100)
                               (take 10 (cycle [42 100 42 1000 2000])))
                :let [puts (map (fn [n] [::xt/put {:xt/id n ::n n}]) (range @ctr (+ @ctr tx-doc-count)))]]
          (xt/submit-tx node puts)
          (swap! ctr + tx-doc-count)))
      (with-open [node (xt/start-node cfg)]
        (xt/sync node)
        (t/is (= #{[@ctr]} (xt/q (xt/db node) '{:find [(count e)] :where [[e ::n]]})))))))

(defn- try-until-true [ms pred]
  (loop [wait-until (+ ms (System/currentTimeMillis))]
    (if (< wait-until (System/currentTimeMillis))
      false
      (or (pred) (recur wait-until)))))

(t/deftest tx-fn-expansion-stats-1825
  (let [node *api*]
    (xt/submit-tx node
                  [[::xt/put {:xt/id :put-foo,
                              :xt/fn '(fn [_ n] [[::xt/put {:xt/id :foo, :answer n}]])}]
                   [::xt/fn :put-foo 42]])
    (xt/sync node)
    (t/is (try-until-true 1000 #(= 1 (:answer (xt/attribute-stats node)))))))
