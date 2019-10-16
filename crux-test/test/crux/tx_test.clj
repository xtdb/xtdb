(ns crux.tx-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures :as f]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :refer [*api*] :as apif]
            [crux.fixtures.kv :as fkv]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.api :as api]
            [crux.rdf :as rdf]
            [crux.query :as q])
  (:import java.util.Date))

(t/use-fixtures :each fs/with-standalone-node fkv/with-kv-dir apif/with-node f/with-silent-test-check)

;; TODO: This is a large, useful, test that exercises many parts, but
;; might be better split up.
(t/deftest test-can-index-tx-ops-acceptance-test
  (let [picasso (:http://dbpedia.org/resource/Pablo_Picasso (->> "crux/Pablo_Picasso.ntriples"
                                                                 (rdf/ntriples)
                                                                 (rdf/->default-language)
                                                                 (rdf/->maps-by-id)))
        content-hash (c/new-id picasso)
        valid-time #inst "2018-05-21"
        eid (c/new-id :http://dbpedia.org/resource/Pablo_Picasso)
        {:crux.tx/keys [tx-time tx-id]}
        (api/submit-tx *api* [[:crux.tx/put picasso valid-time]])
        _ (api/sync *api* tx-time nil)
        expected-entities [(c/map->EntityTx {:eid          eid
                                             :content-hash content-hash
                                             :vt           valid-time
                                             :tt           tx-time
                                             :tx-id        tx-id})]]

    (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
      (t/testing "can see entity at transact and valid time"
        (t/is (= expected-entities
                 (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time tx-time)))
        (t/is (= expected-entities
                 (idx/all-entities snapshot tx-time tx-time))))

      (t/testing "cannot see entity before valid or transact time"
        (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" tx-time)))
        (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time #inst "2018-05-20")))

        (t/is (empty? (idx/all-entities snapshot #inst "2018-05-20" tx-time)))
        (t/is (empty? (idx/all-entities snapshot tx-time #inst "2018-05-20"))))

      (t/testing "can see entity after valid or transact time"
        (t/is (some? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" tx-time)))
        (t/is (some? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time tx-time))))

      (t/testing "can see entity history"
        (t/is (= [(c/map->EntityTx {:eid          eid
                                    :content-hash content-hash
                                    :vt           valid-time
                                    :tt           tx-time
                                    :tx-id        tx-id})]
                 (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (c/new-id new-picasso)
            new-valid-time #inst "2018-05-20"
            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            (api/submit-tx *api* [[:crux.tx/put new-picasso new-valid-time]])
            _ (api/sync *api* new-tx-time nil)]

        (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid          eid
                                      :content-hash new-content-hash
                                      :vt           new-valid-time
                                      :tt           new-tx-time
                                      :tx-id        new-tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))
          (t/is (= [(c/map->EntityTx {:eid          eid
                                      :content-hash new-content-hash
                                      :vt           new-valid-time
                                      :tt           new-tx-time
                                      :tx-id        new-tx-id})] (idx/all-entities snapshot new-valid-time new-tx-time)))

          (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21"))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (c/new-id new-picasso)
            new-valid-time #inst "2018-05-22"
            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            (api/submit-tx *api* [[:crux.tx/put new-picasso new-valid-time]])
            _ (api/sync *api* new-tx-time nil)]

        (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
          (t/is (= [(c/map->EntityTx {:eid          eid
                                      :content-hash new-content-hash
                                      :vt           new-valid-time
                                      :tt           new-tx-time
                                      :tx-id        new-tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))
          (t/is (= [(c/map->EntityTx {:eid          eid
                                      :content-hash content-hash
                                      :vt           valid-time
                                      :tt           tx-time
                                      :tx-id        tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time tx-time)))
          (t/is (= [(c/map->EntityTx {:eid          eid
                                      :content-hash new-content-hash
                                      :vt           new-valid-time
                                      :tt           new-tx-time
                                      :tx-id        new-tx-id})] (idx/all-entities snapshot new-valid-time new-tx-time))))

        (t/testing "can correct entity at earlier valid time"
          (let [new-picasso (assoc picasso :bar :foo)
                new-content-hash (c/new-id new-picasso)
                prev-tx-time new-tx-time
                prev-tx-id new-tx-id
                new-valid-time #inst "2018-05-22"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id   :crux.tx/tx-id}
                (api/submit-tx *api* [[:crux.tx/put new-picasso new-valid-time]])
                _ (api/sync *api* new-tx-time nil)]

            (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
              (t/is (= [(c/map->EntityTx {:eid          eid
                                          :content-hash new-content-hash
                                          :vt           new-valid-time
                                          :tt           new-tx-time
                                          :tx-id        new-tx-id})]
                       (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))
              (t/is (= [(c/map->EntityTx {:eid          eid
                                          :content-hash new-content-hash
                                          :vt           new-valid-time
                                          :tt           new-tx-time
                                          :tx-id        new-tx-id})] (idx/all-entities snapshot new-valid-time new-tx-time)))

              (t/is (= prev-tx-id (-> (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] prev-tx-time prev-tx-time)
                                      (first)
                                      :tx-id))))

            (t/testing "compare and set does nothing with wrong content hash"
              (let [old-picasso (assoc picasso :baz :boz)
                    {cas-failure-tx-time :crux.tx/tx-time}
                    (api/submit-tx *api* [[:crux.tx/cas old-picasso new-picasso new-valid-time]])
                    _ (api/sync *api* cas-failure-tx-time nil)]
                (t/is (= cas-failure-tx-time (tx/await-tx-time (:indexer *api*) cas-failure-tx-time 1000)))
                (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
                  (t/is (= [(c/map->EntityTx {:eid          eid
                                              :content-hash new-content-hash
                                              :vt           new-valid-time
                                              :tt           new-tx-time
                                              :tx-id        new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time cas-failure-tx-time))))))

            (t/testing "compare and set updates with correct content hash"
              (let [old-picasso new-picasso
                    new-picasso (assoc old-picasso :baz :boz)
                    new-content-hash (c/new-id new-picasso)
                    {new-tx-time :crux.tx/tx-time
                     new-tx-id   :crux.tx/tx-id}
                    (api/submit-tx *api* [[:crux.tx/cas old-picasso new-picasso new-valid-time]])]
                (t/is (= new-tx-time (tx/await-tx-time (:indexer *api*) new-tx-time 1000)))
                (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
                  (t/is (= [(c/map->EntityTx {:eid          eid
                                              :content-hash new-content-hash
                                              :vt           new-valid-time
                                              :tt           new-tx-time
                                              :tx-id        new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time))))))

            (t/testing "compare and set can update non existing nil entity"
              (let [new-eid (c/new-id :http://dbpedia.org/resource/Pablo2)
                    new-picasso (assoc new-picasso :crux.db/id :http://dbpedia.org/resource/Pablo2)
                    new-content-hash (c/new-id new-picasso)
                    {new-tx-time :crux.tx/tx-time
                     new-tx-id   :crux.tx/tx-id}
                    (api/submit-tx *api* [[:crux.tx/cas nil new-picasso new-valid-time]])
                    _ (api/sync *api* new-tx-time nil)]
                (t/is (= new-tx-time (tx/await-tx-time (:indexer *api*) new-tx-time 1000)))
                (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
                  (t/is (= [(c/map->EntityTx {:eid          new-eid
                                              :content-hash new-content-hash
                                              :vt           new-valid-time
                                              :tt           new-tx-time
                                              :tx-id        new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo2] new-valid-time new-tx-time))))))))

        (t/testing "can delete entity"
          (let [new-valid-time #inst "2018-05-23"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id   :crux.tx/tx-id}
                (api/submit-tx *api* [[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso new-valid-time]])
                _ (api/sync *api* new-tx-time nil)]
            (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
              (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (-> (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] valid-time new-tx-time)
                                   (first)
                                   :tx-id)))))))))

    (t/testing "can retrieve history of entity"
      (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
        (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
          (t/is (= 6 (count (map :content-hash picasso-history))))
          (with-open [i (kv/new-iterator snapshot)]
            (doseq [{:keys [content-hash]} picasso-history
                    :when (not (= (c/new-id nil) content-hash))
                    :let [version-k (c/encode-attribute+entity+content-hash+value-key-to
                                     nil
                                     (c/->id-buffer :http://xmlns.com/foaf/0.1/givenName)
                                     (c/->id-buffer :http://dbpedia.org/resource/Pablo_Picasso)
                                     (c/->id-buffer content-hash)
                                     (c/->value-buffer "Pablo"))]]
              (t/is (kv/get-value snapshot version-k)))))))

    (t/testing "can evict entity"
      (let [new-valid-time #inst "2018-05-23"

                                        ; read documents before transaction to populate the cache
            _ (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
                (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
                  (db/get-objects (:object-store *api*) snapshot (keep :content-hash picasso-history))))

            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            (api/submit-tx *api* [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso #inst "1970" new-valid-time]])
            _ (api/sync *api* new-tx-time nil)]

        ;; TODO some race condition going on here:
        (Thread/sleep 1000)

        (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
          (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))

          (t/testing "eviction keeps tx history"
            (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
              (t/is (= 6 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (db/get-objects (:object-store *api*) snapshot (keep :content-hash picasso-history))))))))))))

(t/deftest test-can-store-doc
  (let [picasso (:http://dbpedia.org/resource/Pablo_Picasso (->> "crux/Pablo_Picasso.ntriples"
                                                                 (rdf/ntriples)
                                                                 (rdf/->default-language)
                                                                 (rdf/->maps-by-id)))

        content-hash (c/new-id picasso)]
    (t/is (= 48 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (db/submit-doc (:tx-log *api*) content-hash picasso)

    (Thread/sleep 1000)

    (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
      (t/is (= {content-hash picasso}
               (db/get-objects (:object-store *api*) snapshot [content-hash])))

      (t/testing "non existent docs are ignored"
        (t/is (= {content-hash picasso}
                 (db/get-objects (:object-store *api*)
                                 snapshot
                                 [content-hash
                                  "090622a35d4b579d2fcfebf823821298711d3867"])))
        (t/is (empty? (db/get-objects (:object-store *api*) snapshot [])))))))

(t/deftest test-can-correct-ranges-in-the-past
  (let [ivan {:crux.db/id :ivan :name "Ivan"}

        v1-ivan (assoc ivan :version 1)
        v1-valid-time #inst "2018-11-26"
        {v1-tx-time :crux.tx/tx-time
         v1-tx-id :crux.tx/tx-id}
        (api/submit-tx *api* [[:crux.tx/put v1-ivan v1-valid-time]])
        _ (api/sync *api* v1-tx-time nil)

        v2-ivan (assoc ivan :version 2)
        v2-valid-time #inst "2018-11-27"
        {v2-tx-time :crux.tx/tx-time
         v2-tx-id :crux.tx/tx-id}
        (api/submit-tx *api* [[:crux.tx/put v2-ivan v2-valid-time]])
        _ (api/sync *api* v2-tx-time nil)

        v3-ivan (assoc ivan :version 3)
        v3-valid-time #inst "2018-11-28"
        {v3-tx-time :crux.tx/tx-time
         v3-tx-id :crux.tx/tx-id}
        (api/submit-tx *api* [[:crux.tx/put v3-ivan v3-valid-time]])
        _ (api/sync *api* v3-tx-time nil)]

    (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
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

    (let [v4-valid-time #inst "2018-11-30"
          corrected-ivan (assoc ivan :version 4)
          corrected-start-valid-time #inst "2018-11-27"
          corrected-end-valid-time #inst "2018-11-29"
          {corrected-tx-time :crux.tx/tx-time
           corrected-tx-id :crux.tx/tx-id}
          (api/submit-tx *api* [[:crux.tx/put corrected-ivan corrected-start-valid-time corrected-end-valid-time]])
          _ (api/sync *api* corrected-tx-time nil)]

      (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
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
                       (select-keys [:tx-id :content-hash])))))

        (t/testing "fourth version of entity is still valid"
          (t/is (= {:content-hash (c/new-id corrected-ivan)
                    :tx-id corrected-tx-id}
                   (-> (idx/entities-at snapshot [:ivan] v4-valid-time corrected-tx-time)
                       (first)
                       (select-keys [:tx-id :content-hash]))))))

      (let [deleted-start-valid-time #inst "2018-11-25"
            deleted-end-valid-time #inst "2018-11-28"
            {deleted-tx-time :crux.tx/tx-time
             deleted-tx-id :crux.tx/tx-id}
            (api/submit-tx *api* [[:crux.tx/delete :ivan deleted-start-valid-time deleted-end-valid-time]])
            _ (api/sync *api* deleted-tx-time nil)]

        (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
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
              (api/submit-tx *api* [[:crux.tx/delete :ivan v3-valid-time v3-valid-time]])]

          (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
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
  (let [ivan {:crux.db/id :ivan :name "Ivan"}
        start-valid-time #inst "2019"
        number-of-versions 1000
        {tx-time :crux.tx/tx-time}
        (api/submit-tx *api* (vec (for [n (range number-of-versions)]
                                    [:crux.tx/put (assoc ivan :verison n) (Date. (+ (.getTime start-valid-time) (inc (long n))))])))
        _ (api/sync *api* tx-time nil)]

    (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
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

(t/deftest test-can-read-kv-tx-log
  (let [ivan {:crux.db/id :ivan :name "Ivan"}

        tx1-ivan (assoc ivan :version 1)
        tx1-valid-time #inst "2018-11-26"
        {tx1-id :crux.tx/tx-id
         tx1-tx-time :crux.tx/tx-time}
        (api/submit-tx *api* [[:crux.tx/put tx1-ivan tx1-valid-time]])
        _ (api/sync *api* tx1-tx-time nil)

        tx2-ivan (assoc ivan :version 2)
        tx2-petr {:crux.db/id :petr :name "Petr"}
        tx2-valid-time #inst "2018-11-27"
        {tx2-id :crux.tx/tx-id
         tx2-tx-time :crux.tx/tx-time}
        (api/submit-tx *api* [[:crux.tx/put tx2-ivan tx2-valid-time]
                              [:crux.tx/put tx2-petr tx2-valid-time]])
        _ (api/sync *api* tx2-tx-time nil)]

    (with-open [tx-log-context (db/new-tx-log-context (:tx-log *api*))]
      (let [log (db/tx-log (:tx-log *api*) tx-log-context nil)]
        (t/is (not (realized? log)))
        (t/is (= [{:crux.tx/tx-id tx1-id
                   :crux.tx/tx-time tx1-tx-time
                   :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id tx1-ivan) tx1-valid-time]]}
                  {:crux.tx/tx-id tx2-id
                   :crux.tx/tx-time tx2-tx-time
                   :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id tx2-ivan) tx2-valid-time]
                                             [:crux.tx/put (c/new-id :petr) (c/new-id tx2-petr) tx2-valid-time]]}]
                 log))))))

(defn- sync-submit-tx [node tx-ops]
  (let [submitted-tx (api/submit-tx node tx-ops)]
    (api/sync node (:crux.tx/tx-time submitted-tx) nil)
    submitted-tx))

(t/deftest test-can-apply-transaction-fn
  (let [exception (atom nil)
        latest-exception #(let [e @exception]
                            (reset! exception nil)
                            e)
        rethrow-latest-exception (fn []
                                   (throw (latest-exception)))]
    (with-redefs [tx/tx-fns-enabled? true
                  tx/log-tx-fn-error (fn [t & args]
                                       (reset! exception t))]
      (let [v1-ivan {:crux.db/id :ivan :name "Ivan" :age 40}
            update-attribute-fn {:crux.db/id :update-attribute-fn
                                 :crux.db.fn/body
                                 '(fn [db eid k f]
                                    [[:crux.tx/put (update (crux.api/entity db eid) k f)]])}]
        (sync-submit-tx *api* [[:crux.tx/put v1-ivan]
                               [:crux.tx/put update-attribute-fn]])
        (t/is (= v1-ivan (api/entity (api/db *api*) :ivan)))
        (t/is (= update-attribute-fn (api/entity (api/db *api*) :update-attribute-fn)))
        (t/is (nil? (latest-exception)))

        (let [v2-ivan (assoc v1-ivan :age 41)
              inc-ivans-age '{:crux.db/id :inc-ivans-age
                              :crux.db.fn/args [:ivan
                                                :age
                                                inc]}]
          (sync-submit-tx *api* [[:crux.tx/fn :update-attribute-fn inc-ivans-age]])
          (t/is (= v2-ivan (api/entity (api/db *api*) :ivan)))
          (t/is (= inc-ivans-age (api/entity (api/db *api*) :inc-ivans-age)))
          (t/is (nil? (latest-exception)))

          (t/testing "resulting documents are indexed"
            (t/is (= #{[41]} (api/q (api/db *api*)
                                    '[:find age :where [e :name "Ivan"] [e :age age]]))))

          (t/testing "exceptions"
            (t/testing "non existing tx fn"
              (sync-submit-tx *api* '[[:crux.tx/fn :non-existing-fn]])
              (t/is (= v2-ivan (api/entity (api/db *api*) :ivan)))
              (t/is (thrown?  NullPointerException (rethrow-latest-exception))))

            (t/testing "invalid arguments"
              (sync-submit-tx *api* '[[:crux.tx/fn :update-attribute-fn {:crux.db/id :inc-ivans-age
                                                                         :crux.db.fn/args [:ivan
                                                                                           :age
                                                                                           foo]}]])
              (t/is (= inc-ivans-age (api/entity (api/db *api*) :inc-ivans-age)))
              (t/is (thrown? clojure.lang.Compiler$CompilerException (rethrow-latest-exception))))

            (t/testing "invalid results"
              (sync-submit-tx *api* [[:crux.tx/put
                                      {:crux.db/id :invalid-fn
                                       :crux.db.fn/body
                                       '(fn [db]
                                          [[:crux.tx/foo]])}]])
              (sync-submit-tx *api* '[[:crux.tx/fn :invalid-fn]])
              (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Spec assertion failed" (rethrow-latest-exception))))

            (t/testing "exception thrown"
              (sync-submit-tx *api* [[:crux.tx/put
                                      {:crux.db/id :exception-fn
                                       :crux.db.fn/body
                                       '(fn [db]
                                          (throw (RuntimeException. "foo")))}]])
              (sync-submit-tx *api* '[[:crux.tx/fn :exception-fn]])
              (t/is (thrown-with-msg? RuntimeException #"foo" (rethrow-latest-exception))))

            (t/testing "still working after errors"
              (let [v3-ivan (assoc v1-ivan :age 40)]
                (sync-submit-tx *api* '[[:crux.tx/fn :update-attribute-fn {:crux.db/id :dec-ivans-age
                                                                           :crux.db.fn/args [:ivan
                                                                                             :age
                                                                                             dec]}]])
                (t/is (nil? (latest-exception)))
                (t/is (= v3-ivan (api/entity (api/db *api*) :ivan)))))

            (t/testing "function ops can return other function ops"
              (let [v4-ivan (assoc v1-ivan :name "IVAN")
                    returns-fn {:crux.db/id :returns-fn
                                :crux.db.fn/body
                                '(fn [db]
                                   '[[:crux.tx/fn :update-attribute-fn {:crux.db/id :upcase-ivans-name
                                                                        :crux.db.fn/args [:ivan
                                                                                          :name
                                                                                          clojure.string/upper-case]}]])}]
                (sync-submit-tx *api* [[:crux.tx/put returns-fn]])
                (sync-submit-tx *api* [[:crux.tx/fn :returns-fn]])
                (t/is (nil? (latest-exception)))
                (t/is (= v4-ivan (api/entity (api/db *api*) :ivan)))))

            (t/testing "can access current transaction as dynamic var"
              (sync-submit-tx *api*
                              [[:crux.tx/put
                                {:crux.db/id :tx-metadata-fn
                                 :crux.db.fn/body
                                 '(fn [db]
                                    [[:crux.tx/put {:crux.db/id :tx-metadata :crux.tx/current-tx crux.tx/*current-tx*}]])}]])
              (let [submitted-tx (sync-submit-tx *api* '[[:crux.tx/fn :tx-metadata-fn]])]
                (t/is (nil? (latest-exception)))
                (t/is (= {:crux.db/id :tx-metadata
                          :crux.tx/current-tx (assoc submitted-tx :crux.tx.event/tx-events [[:crux.tx/fn (str (c/new-id :tx-metadata-fn))]])}
                         (api/entity (api/db *api*) :tx-metadata)))))))))))
