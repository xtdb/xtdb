(ns crux.tx-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.bus :as bus]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.api :as api]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.node :as n]
            [crux.io :as cio]
            [taoensso.nippy :as nippy]
            [crux.tx.event :as txe]
            [clojure.string :as string]
            [crux.tx.conform :as txc]
            [crux.api :as crux])
  (:import [java.util Date UUID Collections HashMap HashSet]
           [java.time Duration]
           [crux.codec EntityTx]
           [clojure.lang Keyword PersistentArrayMap]
           [java.net URL URI]))

(t/use-fixtures :each fix/with-node fix/with-silent-test-check
  (fn [f]
    (f)
    (#'tx/reset-tx-fn-error)))

(def picasso-id :http://dbpedia.org/resource/Pablo_Picasso)
(def picasso-eid (c/new-id picasso-id))

(def picasso
  (-> "crux/Pablo_Picasso.ntriples"
      (rdf/ntriples)
      (rdf/->default-language)
      (rdf/->maps-by-id)
      (get picasso-id)))

;; TODO: This is a large, useful, test that exercises many parts, but
;; might be better split up.
(t/deftest test-can-index-tx-ops-acceptance-test
  (let [content-hash (c/new-id picasso)
        valid-time #inst "2018-05-21"
        {:crux.tx/keys [tx-time tx-id]}
        (fix/submit+await-tx [[:crux.tx/put picasso valid-time]])]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/testing "can see entity at transact and valid time"
        (t/is (= (c/map->EntityTx {:eid picasso-eid
                                   :content-hash content-hash
                                   :vt valid-time
                                   :tt tx-time
                                   :tx-id tx-id})
                 (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso tx-time tx-id))))

      (t/testing "cannot see entity before valid or transact time"
        (t/is (nil? (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso #inst "2018-05-20" tx-id)))
        (t/is (nil? (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso tx-time -1))))

      (t/testing "can see entity after valid or transact time"
        (t/is (some? (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso #inst "2018-05-22" tx-id)))
        (t/is (some? (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso tx-time tx-id))))

      (t/testing "can see entity history"
        (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                    :content-hash content-hash
                                    :vt valid-time
                                    :tt tx-time
                                    :tx-id tx-id})]
                 (db/entity-history index-snapshot :http://dbpedia.org/resource/Pablo_Picasso :desc {})))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (c/new-id new-picasso)
            new-valid-time #inst "2018-05-20"
            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            (fix/submit+await-tx [[:crux.tx/put new-picasso new-valid-time]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash new-content-hash
                                     :vt new-valid-time
                                     :tt new-tx-time
                                     :tx-id new-tx-id})
                   (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso new-valid-time new-tx-id)))

          (t/is (nil? (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso #inst "2018-05-20" -1))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (c/new-id new-picasso)
            new-valid-time #inst "2018-05-22"
            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            (fix/submit+await-tx [[:crux.tx/put new-picasso new-valid-time]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash new-content-hash
                                     :vt new-valid-time
                                     :tt new-tx-time
                                     :tx-id new-tx-id})
                   (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso new-valid-time new-tx-id)))
          (t/is (= (c/map->EntityTx {:eid picasso-eid
                                     :content-hash content-hash
                                     :vt valid-time
                                     :tt tx-time
                                     :tx-id tx-id})
                   (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso new-valid-time tx-id))))

        (t/testing "can correct entity at earlier valid time"
          (let [new-picasso (assoc picasso :bar :foo)
                new-content-hash (c/new-id new-picasso)
                prev-tx-time new-tx-time
                prev-tx-id new-tx-id
                new-valid-time #inst "2018-05-22"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id   :crux.tx/tx-id}
                (fix/submit+await-tx [[:crux.tx/put new-picasso new-valid-time]])]

            (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
              (t/is (= (c/map->EntityTx {:eid picasso-eid
                                         :content-hash new-content-hash
                                         :vt new-valid-time
                                         :tt new-tx-time
                                         :tx-id new-tx-id})
                       (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso new-valid-time new-tx-id)))

              (t/is (= prev-tx-id
                       (:tx-id (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso prev-tx-time prev-tx-id)))))))

        (t/testing "can delete entity"
          (let [new-valid-time #inst "2018-05-23"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id   :crux.tx/tx-id}
                (fix/submit+await-tx [[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso new-valid-time]])]
            (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
              (t/is (nil? (.content-hash (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso new-valid-time new-tx-id))))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (:tx-id (db/entity-as-of index-snapshot :http://dbpedia.org/resource/Pablo_Picasso valid-time new-tx-id))))))))))

    (t/testing "can retrieve history of entity"
      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= 5 (count (db/entity-history index-snapshot :http://dbpedia.org/resource/Pablo_Picasso :desc
                                             {:with-corrections? true}))))))))

(t/deftest test-can-cas-entity
  (let [{picasso-tx-time :crux.tx/tx-time, picasso-tx-id :crux.tx/tx-id} (api/submit-tx *api* [[:crux.tx/put picasso]])]

    (t/testing "compare and set does nothing with wrong content hash"
      (let [wrong-picasso (assoc picasso :baz :boz)
            cas-failure-tx (api/submit-tx *api* [[:crux.tx/cas wrong-picasso (assoc picasso :foo :bar)]])]

        (api/await-tx *api* cas-failure-tx (Duration/ofMillis 1000))

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {}))))))

    (t/testing "compare and set updates with correct content hash"
      (let [new-picasso (assoc picasso :new? true)
            {new-tx-time :crux.tx/tx-time, new-tx-id :crux.tx/tx-id} (fix/submit+await-tx [[:crux.tx/cas picasso new-picasso]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id new-picasso)
                                      :vt new-tx-time
                                      :tt new-tx-time
                                      :tx-id new-tx-id})
                    (c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {})))))))

  (t/testing "compare and set can update non existing nil entity"
    (let [ivan {:crux.db/id :ivan, :value 12}
          {ivan-tx-time :crux.tx/tx-time, ivan-tx-id :crux.tx/tx-id} (fix/submit+await-tx [[:crux.tx/cas nil ivan]])]

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= [(c/map->EntityTx {:eid (c/new-id :ivan)
                                    :content-hash (c/new-id ivan)
                                    :vt ivan-tx-time
                                    :tt ivan-tx-time
                                    :tx-id ivan-tx-id})]
                 (db/entity-history index-snapshot :ivan :desc {})))))))

(t/deftest test-match-ops
  (let [{picasso-tx-time :crux.tx/tx-time, picasso-tx-id :crux.tx/tx-id} (api/submit-tx *api* [[:crux.tx/put picasso]])]

    (t/testing "match does nothing with wrong content hash"
      (let [wrong-picasso (assoc picasso :baz :boz)
            match-failure-tx (api/submit-tx *api* [[:crux.tx/match picasso-id wrong-picasso]])]

        (api/await-tx *api* match-failure-tx (Duration/ofMillis 1000))

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {}))))))

    (t/testing "match continues with correct content hash"
      (let [new-picasso (assoc picasso :new? true)
            {new-tx-time :crux.tx/tx-time, new-tx-id :crux.tx/tx-id} (fix/submit+await-tx [[:crux.tx/match picasso-id picasso]
                                                                                           [:crux.tx/put new-picasso]])]

        (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id new-picasso)
                                      :vt new-tx-time
                                      :tt new-tx-time
                                      :tx-id new-tx-id})
                    (c/map->EntityTx {:eid picasso-eid
                                      :content-hash (c/new-id picasso)
                                      :vt picasso-tx-time
                                      :tt picasso-tx-time
                                      :tx-id picasso-tx-id})]
                   (db/entity-history index-snapshot picasso-id :desc {})))))))

  (t/testing "match can check non existing entity"
    (let [ivan {:crux.db/id :ivan, :value 12}
          {ivan-tx-time :crux.tx/tx-time, ivan-tx-id :crux.tx/tx-id} (fix/submit+await-tx [[:crux.tx/match :ivan nil]
                                                                                           [:crux.tx/put ivan]])]

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (= [(c/map->EntityTx {:eid (c/new-id :ivan)
                                    :content-hash (c/new-id ivan)
                                    :vt ivan-tx-time
                                    :tt ivan-tx-time
                                    :tx-id ivan-tx-id})]
                 (db/entity-history index-snapshot :ivan :desc {})))))))

(t/deftest test-can-evict-entity
  (t/testing "removes all traces of entity from indices"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :value 0}]])
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :value 1}]])
    (fix/submit+await-tx [[:crux.tx/evict :foo]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [history (db/entity-history index-snapshot :foo :desc {})]
        (t/testing "eviction removes tx history"
          (t/is (empty? history)))
        (t/testing "eviction removes docs"
          (t/is (empty? (->> (db/fetch-docs (:document-store *api*) (keep :content-hash history))
                             vals
                             (remove c/evicted-doc?))))))))

  (t/testing "clears entity history for valid-time ranges"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :bar, :value 0} #inst "2012" #inst "2018"]])
    (fix/submit+await-tx [[:crux.tx/evict :bar]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [history (db/entity-history index-snapshot :bar :desc {})]
        (t/testing "eviction removes tx history"
          (t/is (empty? history)))))))

(defn index-tx [tx tx-events]
  (let [{:keys [tx-ingester]} *api*
        in-flight-tx (db/begin-tx tx-ingester tx nil)]
    (db/index-tx-events in-flight-tx tx-events)
    (db/commit in-flight-tx)))

(t/deftest test-handles-legacy-evict-events
  (let [{put-tx-time ::tx/tx-time, put-tx-id ::tx/tx-id} (fix/submit+await-tx [[:crux.tx/put picasso #inst "2018-05-21"]])

        evict-tx-time #inst "2018-05-22"
        evict-tx-id (inc put-tx-id)

        index-evict! (fn []
                       (index-tx {:crux.tx/tx-time evict-tx-time
                                  :crux.tx/tx-id evict-tx-id}
                                 [[:crux.tx/evict picasso-id #inst "2018-05-23"]]))]

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
        t #inst "2019-11-29"]
    (db/submit-docs (:document-store *api*) {(c/new-id ivan1) ivan1
                                             (c/new-id ivan2) ivan2})

    (index-tx {:crux.tx/tx-time t, :crux.tx/tx-id 1}
              [[:crux.tx/put :ivan (c/->id-buffer (c/new-id ivan1))]])
    (index-tx {:crux.tx/tx-time t, :crux.tx/tx-id 2}
              [[:crux.tx/put :ivan (c/->id-buffer (c/new-id ivan2))]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/new-id ivan2))
                (c/->EntityTx (c/new-id :ivan) t t 1 (c/new-id ivan1))]
               (db/entity-history index-snapshot :ivan :desc {:with-corrections? true})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/new-id ivan2))]
               (db/entity-history index-snapshot :ivan :desc {:start-valid-time t
                                                              :start-tx-id 2})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 1 (c/new-id ivan1))]
               (db/entity-history index-snapshot :ivan :desc {:start-valid-time t
                                                              :start-tx-id 1})))

      (t/is (= [(c/->EntityTx (c/new-id :ivan) t t 2 (c/new-id ivan2))]
               (db/entity-history index-snapshot :ivan :asc {:start-valid-time t}))))))

(t/deftest test-entity-history-seq-corner-cases
  (let [ivan {:crux.db/id :ivan}
        ivan1 (assoc ivan :value 1)
        ivan2 (assoc ivan :value 2)
        t1 #inst "2020-05-01"
        t2 #inst "2020-05-02"]
    (db/submit-docs (:document-store *api*) {(c/new-id ivan1) ivan1
                                             (c/new-id ivan2) ivan2})

    (index-tx {:crux.tx/tx-time t1, :crux.tx/tx-id 1}
              [[:crux.tx/put :ivan (c/->id-buffer (c/new-id ivan1)) t1]])
    (index-tx {:crux.tx/tx-time t2, :crux.tx/tx-id 2}
              [[:crux.tx/put :ivan (c/->id-buffer (c/new-id ivan2)) t1]
               [:crux.tx/put :ivan (c/->id-buffer (c/new-id ivan2))]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [etx-v1-t1 (c/->EntityTx (c/new-id :ivan) t1 t1 1 (c/new-id ivan1))
            etx-v1-t2 (c/->EntityTx (c/new-id :ivan) t1 t2 2 (c/new-id ivan2))
            etx-v2-t2 (c/->EntityTx (c/new-id :ivan) t2 t2 2 (c/new-id ivan2))]
        (letfn [(history-asc [opts]
                  (vec (db/entity-history index-snapshot :ivan :asc opts)))
                (history-desc [opts]
                  (vec (db/entity-history index-snapshot :ivan :desc opts)))]

          (t/testing "start is inclusive"
            (t/is (= [etx-v2-t2
                      etx-v1-t2]
                     (history-desc {:start-tx-id 2, :start-valid-time t2})))

            (t/is (= [etx-v1-t2]
                     (history-desc {:start-valid-time t1})))

            (t/is (= [etx-v1-t2 etx-v2-t2]
                     (history-asc {:start-tx-id 2})))

            (t/is (= [etx-v1-t1 etx-v1-t2 etx-v2-t2]
                     (history-asc {:start-tx-id 1, :start-valid-time t1
                                   :with-corrections? true}))))

          (t/testing "end is exclusive"
            (t/is (= [etx-v2-t2]
                     (history-desc {:start-valid-time t2, :start-tx-id 2
                                    :end-valid-time t1, :end-tx-id 1})))

            (t/is (= []
                     (history-desc {:end-valid-time t2})))

            (t/is (= [etx-v1-t1]
                     (history-asc {:end-tx-id 2})))

            (t/is (= []
                     (history-asc {:start-valid-time t1, :end-tx-id 1})))))))))

(t/deftest test-put-delete-range-semantics
  (t/are [txs history] (let [eid (keyword (gensym "ivan"))
                             ivan {:crux.db/id eid, :name "Ivan"}
                             res (mapv (fn [[value & vts]]
                                         (api/submit-tx *api* [(into (if value
                                                                       [:crux.tx/put (assoc ivan :value value)]
                                                                       [:crux.tx/delete eid])
                                                                     vts)]))
                                       txs)
                             first-vt (ffirst history)
                             last-tx (last res)]

                         (api/await-tx *api* last-tx nil)

                         (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
                           (t/is (= (for [[vt tx-idx value] history]
                                      [vt (get-in res [tx-idx :crux.tx/tx-id]) (c/new-id (when value
                                                                                           (assoc ivan :value value)))])

                                    (->> (db/entity-history index-snapshot eid :asc
                                                            {:start {::db/valid-time first-vt}})
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
  (let [ivan {:crux.db/id :ivan :name "Ivan"}
        start-valid-time #inst "2019"
        number-of-versions 1000
        tx (fix/submit+await-tx (for [n (range number-of-versions)]
                                  [:crux.tx/put (assoc ivan :version n) (Date. (+ (.getTime start-valid-time) (inc (long n))))]))]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [baseline-time (let [start-time (System/nanoTime)
                                valid-time (Date. (+ (.getTime start-valid-time) number-of-versions))]
                            (t/testing "last version of entity is visible at now"
                              (t/is (= valid-time (:vt (db/entity-as-of index-snapshot :ivan valid-time (::tx/tx-id tx))))))
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
  (let [ivan {:crux.db/id :ivan :name "Ivan"}

        tx1-ivan (assoc ivan :version 1)
        tx1-valid-time #inst "2018-11-26"
        {tx1-id :crux.tx/tx-id
         tx1-tx-time :crux.tx/tx-time}
        (fix/submit+await-tx [[:crux.tx/put tx1-ivan tx1-valid-time]])

        tx2-ivan (assoc ivan :version 2)
        tx2-petr {:crux.db/id :petr :name "Petr"}
        tx2-valid-time #inst "2018-11-27"
        {tx2-id :crux.tx/tx-id
         tx2-tx-time :crux.tx/tx-time}
        (fix/submit+await-tx [[:crux.tx/put tx2-ivan tx2-valid-time]
                              [:crux.tx/put tx2-petr tx2-valid-time]])]

    (with-open [log-iterator (db/open-tx-log (:tx-log *api*) nil)]
      (let [log (iterator-seq log-iterator)]
        (t/is (not (realized? log)))
        (t/is (= [{:crux.tx/tx-id tx1-id
                   :crux.tx/tx-time tx1-tx-time
                   :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id tx1-ivan) tx1-valid-time]]}
                  {:crux.tx/tx-id tx2-id
                   :crux.tx/tx-time tx2-tx-time
                   :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id tx2-ivan) tx2-valid-time]
                                             [:crux.tx/put (c/new-id :petr) (c/new-id tx2-petr) tx2-valid-time]]}]
                 log))))))

(t/deftest migrates-unhashed-tx-log-eids
  (let [doc {:crux.db/id :foo}
        doc-id (c/new-id doc)]
    (db/submit-docs (:document-store *api*) {doc-id doc})
    (doto @(db/submit-tx (:tx-log *api*) [[:crux.tx/match :foo (c/new-id c/nil-id-buffer)]
                                          [:crux.tx/put :foo doc-id]])
      (->> (api/await-tx *api*)))

    (t/is (= {:crux.db/id :foo}
             (api/entity (api/db *api*) :foo)))

    (with-open [log-iterator (api/open-tx-log *api* nil nil)]
      (let [evts (::txe/tx-events (first (iterator-seq log-iterator)))]
        ;; have to check not= too because Id's `equals` conforms its input to Id
        (t/is (not= [:crux.tx/match :foo (c/new-id c/nil-id-buffer)]
                    (first evts)))
        (t/is (not= [:crux.tx/put :foo doc-id]
                    (second evts)))
        (t/is (= [[:crux.tx/match (c/new-id :foo) (c/new-id c/nil-id-buffer)]
                  [:crux.tx/put (c/new-id :foo) doc-id]]
                 evts))))

    (fix/submit+await-tx [[:crux.tx/delete :foo]])
    (t/is (nil? (api/entity (api/db *api*) :foo)))))

(t/deftest test-can-apply-transaction-fn
  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (let [v1-ivan {:crux.db/id :ivan :name "Ivan" :age 40}
          v4-ivan (assoc v1-ivan :name "IVAN")
          update-attribute-fn {:crux.db/id :update-attribute-fn
                               :crux.db/fn '(fn [ctx eid k f]
                                              [[:crux.tx/put (update (crux.api/entity (crux.api/db ctx) eid) k (eval f))]])}]
      (fix/submit+await-tx [[:crux.tx/put v1-ivan]
                            [:crux.tx/put update-attribute-fn]])
      (t/is (= v1-ivan (api/entity (api/db *api*) :ivan)))
      (t/is (= update-attribute-fn (api/entity (api/db *api*) :update-attribute-fn)))
      (some-> (#'tx/reset-tx-fn-error) throw)

      (let [v2-ivan (assoc v1-ivan :age 41)]
        (fix/submit+await-tx [[:crux.tx/fn :update-attribute-fn :ivan :age `inc]])
        (some-> (#'tx/reset-tx-fn-error) throw)
        (t/is (= v2-ivan (api/entity (api/db *api*) :ivan)))

        (t/testing "resulting documents are indexed"
          (t/is (= #{[41]} (api/q (api/db *api*)
                                  '[:find age :where [e :name "Ivan"] [e :age age]]))))

        (t/testing "exceptions"
          (t/testing "non existing tx fn"
            (fix/submit+await-tx '[[:crux.tx/fn :non-existing-fn]])
            (t/is (= v2-ivan (api/entity (api/db *api*) :ivan)))
            (t/is (thrown? NullPointerException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "invalid arguments"
            (fix/submit+await-tx '[[:crux.tx/fn :update-attribute-fn :ivan :age foo]])
            (t/is (thrown? clojure.lang.Compiler$CompilerException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "invalid results"
            (fix/submit+await-tx [[:crux.tx/put
                                   {:crux.db/id :invalid-fn
                                    :crux.db/fn '(fn [ctx]
                                                   [[:crux.tx/foo]])}]])
            (fix/submit+await-tx '[[:crux.tx/fn :invalid-fn]])
            (t/is (thrown-with-msg? IllegalArgumentException #"Invalid tx op" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "no :crux.db/fn"
            (fix/submit+await-tx [[:crux.tx/put
                                   {:crux.db/id :no-fn}]])
            (fix/submit+await-tx '[[:crux.tx/fn :no-fn]])
            (t/is (thrown? NullPointerException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "not a fn"
            (fix/submit+await-tx [[:crux.tx/put
                                   {:crux.db/id :not-a-fn
                                    :crux.db/fn 0}]])
            (fix/submit+await-tx '[[:crux.tx/fn :not-a-fn]])
            (t/is (thrown? ClassCastException (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "compilation errors"
            (fix/submit+await-tx [[:crux.tx/put
                                   {:crux.db/id :compilation-error-fn
                                    :crux.db/fn '(fn [ctx]
                                                   unknown-symbol)}]])
            (fix/submit+await-tx '[[:crux.tx/fn :compilation-error-fn]])
            (t/is (thrown-with-msg? Exception #"Syntax error compiling" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "exception thrown"
            (fix/submit+await-tx [[:crux.tx/put
                                   {:crux.db/id :exception-fn
                                    :crux.db/fn '(fn [ctx]
                                                   (throw (RuntimeException. "foo")))}]])
            (fix/submit+await-tx '[[:crux.tx/fn :exception-fn]])
            (t/is (thrown-with-msg? RuntimeException #"foo" (some-> (#'tx/reset-tx-fn-error) throw))))

          (t/testing "still working after errors"
            (let [v3-ivan (assoc v1-ivan :age 40)]
              (fix/submit+await-tx [[:crux.tx/fn :update-attribute-fn :ivan :age `dec]])
              (some-> (#'tx/reset-tx-fn-error) throw)
              (t/is (= v3-ivan (api/entity (api/db *api*) :ivan))))))

        (t/testing "sees in-transaction version of entities (including itself)"
          (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo 1}]])
          (let [tx (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo 2}]
                                         [:crux.tx/put {:crux.db/id :doubling-fn
                                                        :crux.db/fn '(fn [ctx]
                                                                       [[:crux.tx/put (-> (crux.api/entity (crux.api/db ctx) :foo)
                                                                                          (update :foo * 2))]])}]
                                         [:crux.tx/fn :doubling-fn]])]

            (t/is (crux/tx-committed? *api* tx))
            (t/is (= {:crux.db/id :foo, :foo 4}
                     (crux/entity (crux/db *api*) :foo)))))

        (t/testing "function ops can return other function ops"
          (let [returns-fn {:crux.db/id :returns-fn
                            :crux.db/fn '(fn [ctx]
                                           [[:crux.tx/fn :update-attribute-fn :ivan :name `string/upper-case]])}]
            (fix/submit+await-tx [[:crux.tx/put returns-fn]])
            (fix/submit+await-tx [[:crux.tx/fn :returns-fn]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= v4-ivan (api/entity (api/db *api*) :ivan)))))

        (t/testing "repeated 'merge' operation behaves correctly"
          (let [v5-ivan (merge v4-ivan
                               {:height 180
                                :hair-style "short"
                                :mass 60})
                merge-fn {:crux.db/id :merge-fn
                          :crux.db/fn `(fn [ctx# eid# m#]
                                         [[:crux.tx/put (merge (api/entity (api/db ctx#) eid#) m#)]])}]
            (fix/submit+await-tx [[:crux.tx/put merge-fn]])
            (fix/submit+await-tx [[:crux.tx/fn :merge-fn :ivan {:mass 60, :hair-style "short"}]])
            (fix/submit+await-tx [[:crux.tx/fn :merge-fn :ivan {:height 180}]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= v5-ivan (api/entity (api/db *api*) :ivan)))))

        (t/testing "function ops can return other function ops that also the forked ctx"
          (let [returns-fn {:crux.db/id :returns-fn
                            :crux.db/fn '(fn [ctx]
                                           [[:crux.tx/put {:crux.db/id :ivan :name "modified ivan"}]
                                            [:crux.tx/fn :update-attribute-fn :ivan :name `string/upper-case]])}]
            (fix/submit+await-tx [[:crux.tx/put returns-fn]])
            (fix/submit+await-tx [[:crux.tx/fn :returns-fn]])
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= {:crux.db/id :ivan :name "MODIFIED IVAN"}
                     (api/entity (api/db *api*) :ivan)))))

        (t/testing "can access current transaction on tx-fn context"
          (fix/submit+await-tx
           [[:crux.tx/put
             {:crux.db/id :tx-metadata-fn
              :crux.db/fn `(fn [ctx#]
                             [[:crux.tx/put {:crux.db/id :tx-metadata :crux.tx/current-tx (api/indexing-tx ctx#)}]])}]])
          (let [submitted-tx (fix/submit+await-tx '[[:crux.tx/fn :tx-metadata-fn]])]
            (some-> (#'tx/reset-tx-fn-error) throw)
            (t/is (= {:crux.db/id :tx-metadata
                      :crux.tx/current-tx submitted-tx}
                     (api/entity (api/db *api*) :tx-metadata)))))))))

(t/deftest tx-fn-sees-in-tx-query-results
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo 1}]])
  (let [tx (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo 2}]
                                 [:crux.tx/put {:crux.db/id :put
                                                :crux.db/fn '(fn [ctx doc]
                                                               [[:crux.tx/put doc]])}]
                                 [:crux.tx/fn :put {:crux.db/id :bar, :ref :foo}]
                                 [:crux.tx/put {:crux.db/id :doubling-fn
                                                :crux.db/fn '(fn [ctx]
                                                               (let [db (crux.api/db ctx)]

                                                                 [[:crux.tx/put {:crux.db/id :prn-out
                                                                                 :e (crux.api/entity db :bar)
                                                                                 :q (first (crux.api/q db {:find '[e v]
                                                                                                           :where '[[e :crux.db/id :bar]
                                                                                                                    [e :ref v]]}))}]
                                                                  [:crux.tx/put (-> (crux.api/entity db :foo)
                                                                                    (update :foo * 2))]]))}]
                                 [:crux.tx/fn :doubling-fn]])]

    (t/is (crux/tx-committed? *api* tx))
    (t/is (= {:crux.db/id :foo, :foo 4}
             (crux/entity (crux/db *api*) :foo)))
    (t/is (= {:crux.db/id :prn-out
              :e {:crux.db/id :bar :ref :foo}
              :q [:bar :foo]}
             (crux/entity (crux/db *api*) :prn-out)))))

(t/deftest tx-log-evict-454 []
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :to-evict}]])
  (fix/submit+await-tx [[:crux.tx/cas {:crux.db/id :to-evict} {:crux.db/id :to-evict :test "test"}]])
  (fix/submit+await-tx [[:crux.tx/evict :to-evict]])

  (with-open [log-iterator (api/open-tx-log *api* nil true)]
    (t/is (= [[[:crux.tx/put
                #:crux.db{:id #crux/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                          :evicted? true}]]
              [[:crux.tx/cas
                #:crux.db{:id #crux/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                          :evicted? true}
                #:crux.db{:id #crux/id "6abe906510aa2263737167c12c252245bdcf6fb0",
                          :evicted? true}]]
              [[:crux.tx/evict
                #crux/id "6abe906510aa2263737167c12c252245bdcf6fb0"]]]
             (->> (iterator-seq log-iterator)
                  (map :crux.api/tx-ops))))))

(t/deftest transaction-fn-return-values-457
  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (let [nil-fn {:crux.db/id :nil-fn
                  :crux.db/fn '(fn [ctx] nil)}
          false-fn {:crux.db/id :false-fn
                    :crux.db/fn '(fn [ctx] false)}]

      (fix/submit+await-tx [[:crux.tx/put nil-fn]
                            [:crux.tx/put false-fn]])
      (fix/submit+await-tx [[:crux.tx/fn :nil-fn]
                            [:crux.tx/put {:crux.db/id :foo
                                           :foo? true}]])

      (t/is (= {:crux.db/id :foo, :foo? true}
               (api/entity (api/db *api*) :foo)))

      (fix/submit+await-tx [[:crux.tx/fn :false-fn]
                            [:crux.tx/put {:crux.db/id :bar
                                           :bar? true}]])

      (t/is (nil? (api/entity (api/db *api*) :bar))))))

(t/deftest map-ordering-362
  (t/testing "cas is independent of map ordering"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo :bar}]])
    (fix/submit+await-tx [[:crux.tx/cas {:foo :bar, :crux.db/id :foo} {:crux.db/id :foo, :foo :baz}]])

    (t/is (= {:crux.db/id :foo, :foo :baz}
             (api/entity (api/db *api*) :foo))))

  (t/testing "entities with map keys can be retrieved regardless of ordering"
    (let [doc {:crux.db/id {:foo 1, :bar 2}}]
      (fix/submit+await-tx [[:crux.tx/put doc]])

      (t/is (= doc (api/entity (api/db *api*) {:foo 1, :bar 2})))
      (t/is (= doc (api/entity (api/db *api*) {:bar 2, :foo 1})))))

  (t/testing "entities with map values can be joined regardless of ordering"
    (let [doc {:crux.db/id {:foo 2, :bar 4}}]
      (fix/submit+await-tx [[:crux.tx/put doc]
                            [:crux.tx/put {:crux.db/id :baz, :joins {:bar 4, :foo 2}}]
                            [:crux.tx/put {:crux.db/id :quux, :joins {:foo 2, :bar 4}}]])

      (t/is (= #{[{:foo 2, :bar 4} :baz]
                 [{:foo 2, :bar 4} :quux]}
               (api/q (api/db *api*) '{:find [parent child]
                                       :where [[parent :crux.db/id _]
                                               [child :joins parent]]}))))))

(t/deftest incomparable-colls-1001
  (t/testing "can store and retrieve incomparable colls (#1001)"
    (let [foo {:crux.db/id :foo
               :foo {:bar #{7 "hello"}}}]
      (fix/submit+await-tx [[:crux.tx/put foo]])

      (t/is (= #{[foo]}
               (api/q (api/db *api*) '{:find [(pull ?e [*])]
                                       :where [[?e :foo {:bar #{"hello" 7}}]]}))))

    (let [foo {:crux.db/id :foo
               :foo {{:foo 1} :foo1
                     {:foo 2} :foo2}}]
      (fix/submit+await-tx [[:crux.tx/put foo]])

      (t/is (= #{[foo]}
               (api/q (api/db *api*) '{:find [(pull ?e [*])]
                                       :where [[?e :foo {{:foo 2} :foo2, {:foo 1} :foo1}]]}))))))

(t/deftest test-java-ids-and-values-1398
  (letfn [(test-id [id]
            (t/testing "As ID"
              (with-open [node (crux/start-node {})]
                (let [doc {:crux.db/id id}]
                  (fix/submit+await-tx node [[:crux.tx/put doc]])
                  (t/is (= doc (crux/entity (crux/db node) id)))
                  (t/is #{[id]} (crux/q (crux/db node) '{:find [?id]
                                                         :where [[?id :crux.db/id]]}))))))
          (test-value [value]
            (t/testing "As Value"
              (with-open [node (crux/start-node {})]
                (let [doc {:crux.db/id :foo :bar value}]
                  (fix/submit+await-tx node [[:crux.tx/put doc]])
                  (t/is (= doc (crux/entity (crux/db node) :foo)))
                  (t/is #{[value]} (crux/q (crux/db node) '{:find [?val]
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
      (doto (URI/create "mailto:crux@juxt.pro")
        (test-id) (test-value)))

    (t/testing "URL"
      (doto (URL. "https://github.com/juxt/crux")
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
          (with-open [node (crux/start-node {})]
            (let [doc {:crux.db/id val1}]
              (fix/submit+await-tx node [[:crux.tx/put doc]])
              (t/is (= doc (crux/entity (crux/db node) val2)))
              (let [result (crux/q (crux/db node) '{:find [?id]
                                                    :where [[?id :crux.db/id]]})]
                (t/is #{[val1]} result)
                (t/is #{[val2]} result)))))

        (t/testing "As Value"
          (with-open [node (crux/start-node {})]
            (let [doc {:crux.db/id :foo :bar val1}]
              (fix/submit+await-tx node [[:crux.tx/put doc]])
              (t/is (= doc (crux/entity (crux/db node) :foo)))
              (let [result (crux/q (crux/db node) '{:find [?val]
                                                    :where [[?id :bar ?val]]})]
                (t/is #{[val1]} result)
                (t/is #{[val2]} result)))))))))

(t/deftest overlapping-valid-time-ranges-434
  (let [_ (fix/submit+await-tx
           [[:crux.tx/put {:crux.db/id :foo, :v 10} #inst "2020-01-10"]
            [:crux.tx/put {:crux.db/id :bar, :v 5} #inst "2020-01-05"]
            [:crux.tx/put {:crux.db/id :bar, :v 10} #inst "2020-01-10"]

            [:crux.tx/put {:crux.db/id :baz, :v 10} #inst "2020-01-10"]])

        last-tx (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :bar, :v 7} #inst "2020-01-07"]
                                      ;; mixing foo and bar shouldn't matter
                                      [:crux.tx/put {:crux.db/id :foo, :v 8} #inst "2020-01-08" #inst "2020-01-12"] ; reverts to 10 afterwards
                                      [:crux.tx/put {:crux.db/id :foo, :v 9} #inst "2020-01-09" #inst "2020-01-11"] ; reverts to 8 afterwards, then 10
                                      [:crux.tx/put {:crux.db/id :bar, :v 8} #inst "2020-01-08" #inst "2020-01-09"] ; reverts to 7 afterwards
                                      [:crux.tx/put {:crux.db/id :bar, :v 11} #inst "2020-01-11" #inst "2020-01-12"] ; reverts to 10 afterwards
                                      ])

        db (api/db *api*)]

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (let [eid->history (fn [eid]
                           (let [history (db/entity-history index-snapshot
                                                            (c/new-id eid) :asc
                                                            {:start {::db/valid-time #inst "2020-01-01"}})
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
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :foo :bar}]
                        [:crux.tx/put {:crux.db/id :frob :foo :bar}]])

  (fix/submit+await-tx [[:crux.tx/cas {:crux.db/id :foo, :foo :baz} {:crux.db/id :foo, :foo :quux}]
                        [:crux.tx/put {:crux.db/id :frob :foo :baz}]])
  (fix/submit+await-tx [[:crux.tx/evict :foo]])

  (t/is (every? (comp c/evicted-doc? val)
                (db/fetch-docs (:document-store *api*) #{(c/new-id {:crux.db/id :foo, :foo :bar})
                                                         (c/new-id {:crux.db/id :foo, :foo :baz})
                                                         (c/new-id {:crux.db/id :foo, :foo :quux})})))

  (t/testing "even though the CaS was unrelated, the whole transaction fails - we should still evict those docs"
    (fix/submit+await-tx [[:crux.tx/evict :frob]])
    (t/is (every? (comp c/evicted-doc? val)
                  (db/fetch-docs (:document-store *api*) #{(c/new-id {:crux.db/id :frob, :foo :bar})
                                                           (c/new-id {:crux.db/id :frob, :foo :baz})})))))

(t/deftest raises-tx-events-422
  (let [!events (atom [])
        !latch (promise)]
    (bus/listen (:bus *api*) {:crux/event-types #{::tx/indexing-tx ::tx/committing-tx ::tx/indexed-tx}}
                (fn [evt]
                  (swap! !events conj evt)
                  (when (= ::tx/indexed-tx (:crux/event-type evt))
                    (deliver !latch @!events))))

    (let [doc-1 {:crux.db/id :foo, :value 1}
          doc-2 {:crux.db/id :bar, :value 2}
          doc-ids #{(c/new-id doc-1) (c/new-id doc-2)}
          submitted-tx (fix/submit+await-tx [[:crux.tx/put doc-1] [:crux.tx/put doc-2]])]

      (when (= ::timeout (deref !latch 500 ::timeout))
        (t/is false))

      (t/is (= [{:crux/event-type ::tx/indexing-tx, :submitted-tx submitted-tx}
                {:crux/event-type ::tx/committing-tx,
                 :submitted-tx submitted-tx,
                 :evicting-eids #{}
                 :doc-ids doc-ids}
                {:crux/event-type ::tx/indexed-tx,
                 :submitted-tx submitted-tx,
                 :committed? true
                 :doc-ids doc-ids
                 :av-count 4
                 ::txe/tx-events [[:crux.tx/put
                                   #crux/id "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"
                                   #crux/id "974e28e6484fb6c66e5ca6444ec616207800d815"]
                                  [:crux.tx/put
                                   #crux/id "62cdb7020ff920e5aa642c3d4066950dd1f01f4d"
                                   #crux/id "f2cb628efd5123743c30137b08282b9dee82104a"]]}]
               (-> (vec @!events)
                   (update 2 dissoc :bytes-indexed)))))))

(t/deftest await-fails-quickly-738
  (with-redefs [tx/index-tx-event (fn [_ _ _]
                                    (Thread/sleep 100)
                                    (throw (ex-info "test error for await-fails-quickly-738" {})))]
    (let [last-tx (crux/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
      (t/testing "Testing fail while we are awaiting an event"
        (t/is (thrown-with-msg?
               Exception
               #"Transaction ingester aborted"
               (crux/await-tx *api* last-tx (Duration/ofMillis 1000)))))
      (t/testing "Testing fail before we await an event"
        (t/is (thrown-with-msg?
               Exception
               #"Transaction ingester aborted"
               (crux/await-tx *api* last-tx (Duration/ofMillis 1000))))))))

(t/deftest test-evict-documents-with-common-attributes
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :count 1}]
                        [:crux.tx/put {:crux.db/id :bar, :count 1}]])

  (fix/submit+await-tx [[:crux.tx/evict :foo]])

  (t/is (= #{[:bar]}
           (api/q (api/db *api*) '{:find [?e]
                                   :where [[?e :count 1]]}))))

(t/deftest replaces-tx-fn-arg-docs
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put-ivan
                                       :crux.db/fn '(fn [ctx doc]
                                                      [[:crux.tx/put (assoc doc :crux.db/id :ivan)]])}]])
  (t/testing "replaces args doc with resulting ops"
    (fix/submit+await-tx [[:crux.tx/fn :put-ivan {:name "Ivan"}]])

    (t/is (= {:crux.db/id :ivan, :name "Ivan"}
             (api/entity (api/db *api*) :ivan)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan, :name "Ivan"})]]}
               (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                   (get arg-doc-id)
                   (dissoc :crux.db/id))))))

  (t/testing "replaces fn with no args"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :no-args
                                         :crux.db/fn '(fn [ctx]
                                                        [[:crux.tx/put {:crux.db/id :no-fn-args-doc}]])}]])
    (fix/submit+await-tx [[:crux.tx/fn :no-args]])

    (t/is (= {:crux.db/id :no-fn-args-doc}
             (api/entity (api/db *api*) :no-fn-args-doc)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :no-fn-args-doc) (c/new-id {:crux.db/id :no-fn-args-doc})]]}
               (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                   (get arg-doc-id)
                   (dissoc :crux.db/id))))))

  (t/testing "nested tx-fn"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put-bob-and-ivan
                                         :crux.db/fn '(fn [ctx bob ivan]
                                                        [[:crux.tx/put (assoc bob :crux.db/id :bob)]
                                                         [:crux.tx/fn :put-ivan ivan]])}]])

    (fix/submit+await-tx [[:crux.tx/fn :put-bob-and-ivan {:name "Bob"} {:name "Ivan2"}]])

    (t/is (= {:crux.db/id :ivan, :name "Ivan2"}
             (api/entity (api/db *api*) :ivan)))

    (t/is (= {:crux.db/id :bob, :name "Bob"}
             (api/entity (api/db *api*) :bob)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) 1)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))
          arg-doc (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                      (get arg-doc-id))

          sub-arg-doc-id (-> arg-doc :crux.db.fn/tx-events second last)
          sub-arg-doc (-> (db/fetch-docs (:document-store *api*) #{sub-arg-doc-id})
                          (get sub-arg-doc-id))]

      (t/is (= {:crux.db/id (:crux.db/id arg-doc)
                :crux.db.fn/tx-events [[:crux.tx/put (c/new-id :bob) (c/new-id {:crux.db/id :bob, :name "Bob"})]
                                       [:crux.tx/fn (c/new-id :put-ivan) sub-arg-doc-id]]}
               arg-doc))

      (t/is (= {:crux.db/id (:crux.db/id sub-arg-doc)
                :crux.db.fn/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan2"})]]}
               sub-arg-doc))))

  (t/testing "copes with args doc having been replaced"
    (let [sergei {:crux.db/id :sergei
                  :name "Sergei"}
          arg-doc {:crux.db/id :args
                   :crux.db.fn/tx-events [[:crux.tx/put :sergei (c/new-id sergei)]]}]
      (db/submit-docs (:document-store *api*)
                      {(c/new-id arg-doc) arg-doc
                       (c/new-id sergei) sergei})
      (let [tx @(db/submit-tx (:tx-log *api*) [[:crux.tx/fn :put-sergei (c/new-id arg-doc)]])]
        (api/await-tx *api* tx)

        (t/is (= sergei (api/entity (api/db *api*) :sergei))))))

  (t/testing "failed tx-fn"
    (fix/submit+await-tx [[:crux.tx/fn :put-petr {:name "Petr"}]])

    (t/is (nil? (api/entity (api/db *api*) :petr)))

    (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                       (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

      (t/is (= {:crux.db.fn/failed? true
                :crux.db.fn/exception 'java.lang.NullPointerException
                :crux.db.fn/message nil
                :crux.db.fn/ex-data nil}
               (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                   (get arg-doc-id)
                   (dissoc :crux.db/id)))))))

(t/deftest handles-legacy-tx-fns-with-no-args-doc
  ;; we used to not submit args docs if the tx-fn was 0-arg, and hence had nothing to replace
  ;; we don't want the fix to assume that all tx-fns on the tx-log have an arg-doc
  (let [tx-fn {:crux.db/id :tx-fn,
               :crux.db/fn '(fn [ctx]
                              [[:crux.tx/put {:crux.db/id :ivan}]])}]
    (db/submit-docs (:document-store *api*) {(c/new-id tx-fn) tx-fn})

    (index-tx {:crux.tx/tx-time (Date.), :crux.tx/tx-id 0}
              [[:crux.tx/put (c/new-id :tx-fn) tx-fn]])

    (index-tx {:crux.tx/tx-time (Date.), :crux.tx/tx-id 1}
              [[:crux.tx/fn :tx-fn]])

    (t/is (= {:crux.db/id :ivan}
             (api/entity (api/db *api*) :ivan)))))

(t/deftest test-tx-fn-doc-race-1049
  (let [put-fn {:crux.db/id :put-fn
                :crux.db/fn '(fn [ctx doc]
                               [[:crux.tx/put doc]])}
        foo-doc {:crux.db/id :foo}
        !arg-doc-resps (atom [{:crux.db.fn/args [foo-doc]}
                              {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :foo) (c/new-id foo-doc)]]}])
        ->mocked-doc-store (fn [_opts]
                             (reify db/DocumentStore
                               (submit-docs [_ docs])
                               (fetch-docs [_ ids]
                                 (->> ids
                                      (into {} (map (juxt identity
                                                          (some-fn {(c/new-id put-fn) put-fn
                                                                    (c/new-id foo-doc) foo-doc}
                                                                   (fn [id]
                                                                     (let [[[doc] _] (swap-vals! !arg-doc-resps rest)]
                                                                       (merge doc {:crux.db/id id})))))))))))]
    (with-open [node (api/start-node {:crux/document-store ->mocked-doc-store})]
      (api/submit-tx node [[:crux.tx/put put-fn]])
      (api/submit-tx node [[:crux.tx/fn :put-fn foo-doc]])
      (api/sync node)
      (t/is (= #{[:put-fn] [:foo]}
               (api/q (api/db node) '{:find [?e]
                                      :where [[?e :crux.db/id]]}))))))

(t/deftest ensure-tx-fn-arg-docs-replaced-even-when-tx-aborts-960
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo
                                       :crux.db/fn '(fn [ctx arg] [])}]
                        [:crux.tx/match :foo {:crux.db/id :foo}]
                        [:crux.tx/fn :foo :foo-arg]])

  (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *api*) nil)]
                     (-> (iterator-seq tx-log)
                         first
                         :crux.tx.event/tx-events
                         last
                         last))]
    (t/is (= {:crux.db.fn/failed? true}
             (-> (db/fetch-docs (:document-store *api*) #{arg-doc-id})
                 (get arg-doc-id)
                 (dissoc :crux.db/id))))))

(t/deftest test-documents-with-int-short-byte-float-eids-1043
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id (int 10), :name "foo"}]
                        [:crux.tx/put {:crux.db/id (short 12), :name "bar"}]
                        [:crux.tx/put {:crux.db/id (byte 16), :name "baz"}]
                        [:crux.tx/put {:crux.db/id (float 1.1), :name "quux"}]])

  (let [db (crux/db *api*)]
    (t/is (= #{["foo"] ["bar"] ["baz"] ["quux"]}
             (crux/q (crux/db *api*)
                     '{:find [?name]
                       :where [[?e :name ?name]]})))

    (t/is (= {:crux.db/id (long 10), :name "foo"}
             (crux/entity db (int 10))))

    (t/is (= {:crux.db/id (long 10), :name "foo"}
             (crux/entity db (long 10))))

    (t/is (= #{[10 "foo"]}
             (crux/q (crux/db *api*)
                     {:find '[?e ?name]
                      :where '[[?e :name ?name]]
                      :args [{:?e (int 10)}]}))))

  (t/testing "10 as int and long are the same key"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id 10, :name "foo2"}]])

    (t/is (= #{[10 "foo2"]}
             (crux/q (crux/db *api*)
                     {:find '[?e ?name]
                      :where '[[?e :name ?name]]
                      :args [{:?e (int 10)}]})))))

(t/deftest test-put-evict-in-same-transaction-1337
  (t/testing "put then evict"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test1/a, :test1? true}]])
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test1/b, :test1? true, :test1/evicted? true}]
                          [:crux.tx/evict :test1/b]])
    (let [db (crux/db *api*)]
      (t/is (= {:crux.db/id :test1/a, :test1? true} (crux/entity db :test1/a)))
      (t/is (nil? (crux/entity db :test1/b)))

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (empty? (db/av index-snapshot :test1/evicted? nil)))
        (t/is (empty? (db/entity-history index-snapshot :test1/b :asc {}))))

      (t/is (= #{[:test1/a]} (crux/q db '{:find [?e], :where [[?e :test1? true]]})))
      (t/is (= #{} (crux/q db '{:find [?e], :where [[?e :test1/evicted? true]]})))))

  (t/testing "put then evict an earlier entity"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test2/a, :test2? true, :test2/evicted? true}]])
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test2/b, :test2? true}]
                          [:crux.tx/evict :test2/a]])
    (let [db (crux/db *api*)]
      (t/is (nil? (crux/entity db :test2/a)))
      (t/is (= {:crux.db/id :test2/b, :test2? true} (crux/entity db :test2/b)))

      (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
        (t/is (empty? (db/av index-snapshot :test2/evicted? nil)))
        (t/is (empty? (db/entity-history index-snapshot :test2/a :asc {}))))

      (t/is (= #{[:test2/b]} (crux/q db '{:find [?e], :where [[?e :test2? true]]})))
      (t/is (= #{} (crux/q db '{:find [?e], :where [[?e :test2/evicted? true]]})))))

  (t/testing "evict then put"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test3/a, :test3? true}]])

    (fix/submit+await-tx [[:crux.tx/evict :test3/a]
                          [:crux.tx/put {:crux.db/id :test3/b, :test3? true}]])
    (let [db (crux/db *api*)]
      (t/is (nil? (crux/entity db :test3/a)))
      (t/is (= {:crux.db/id :test3/b, :test3? true} (crux/entity db :test3/b)))
      (t/is (= #{[:test3/b]} (crux/q db '{:find [?e], :where [[?e :test3? true]]})))))

  ;; TODO fails, see #1337
  #_
  (t/testing "evict then re-put"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :test4, :test4? true}]])

    (fix/submit+await-tx [[:crux.tx/evict :test4]
                          [:crux.tx/put {:crux.db/id :test4, :test4? true}]])
    (let [db (crux/db *api*)]
      (t/is (= {:crux.db/id :test4, :test4? true} (crux/entity db :test4)))
      (t/is (= #{[:test4]} (crux/q db '{:find [?e], :where [[?e :test4? true]]}))))))


(t/deftest test-avs-only-shared-by-evicted-entities-1338
  (t/testing "only one entity, AV removed"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :evict-me? true}]])
    (fix/submit+await-tx [[:crux.tx/evict :foo]])

    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (empty? (db/av index-snapshot :evict-me? nil)))))

  ;; TODO fails, see #1338
  #_
  (t/testing "shared by multiple evicted entities"
    (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :evict-me? true}]
                          [:crux.tx/put {:crux.db/id :bar, :evict-me? true}]])
    (fix/submit+await-tx [[:crux.tx/evict :foo]
                          [:crux.tx/evict :bar]])
    (with-open [index-snapshot (db/open-index-snapshot (:index-store *api*))]
      (t/is (empty? (db/av index-snapshot :evict-me? nil))))))

(t/deftest node-shutdown-interrupts-tx-ingestion
  (let [op-count 10
        !calls (atom 0)
        node (crux/start-node {})]
    (with-redefs [tx/index-tx-event (let [f tx/index-tx-event]
                                      (fn [& args]
                                        (let [target-time (+ (System/currentTimeMillis) 200)]
                                          (while (< (System/currentTimeMillis) target-time)))
                                        (swap! !calls inc)
                                        (apply f args)))]
      @(try
         (let [tx (crux/submit-tx node (repeat op-count [:crux.tx/put {:crux.db/id :foo}]))
               await-fut (future
                           (t/is (thrown? InterruptedException (crux/await-tx node tx))))]
           (Thread/sleep 100) ; to ensure the await starts before the node closes
           await-fut)
         (finally
           (.close node))))
    (t/is (instance? InterruptedException (db/ingester-error (:tx-ingester node))))
    (t/is (< @!calls op-count))))

(t/deftest empty-tx-can-be-awaited-1519
  (let [tx (api/submit-tx *api* [])
        _ (api/await-tx *api* tx (Duration/ofSeconds 1))]
    (t/is (= (select-keys tx [::tx/tx-id]) (api/latest-submitted-tx *api*)))
    (t/is (= tx (api/latest-completed-tx *api*)))
    (t/is (api/tx-committed? *api* tx))))
