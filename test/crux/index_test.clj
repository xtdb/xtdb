(ns crux.index-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [crux.byte-utils :as bu]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.fixtures :as f]
            [taoensso.nippy :as nippy])
  (:import java.util.Date
           java.nio.ByteBuffer))

(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-id %)))))

(t/deftest test-can-store-doc
  (let [tx-log (tx/->KvTxLog f/*kv*)
        object-store (idx/->KvObjectStore f/*kv*)
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

;; TODO: This is a large, useful, test that exercises many parts, but
;; might be better split up.
(t/deftest test-can-index-tx-ops-acceptance-test
  (let [tx-log (tx/->KvTxLog f/*kv*)
        object-store (idx/->KvObjectStore f/*kv*)
        indexer (tx/->KvIndexer f/*kv* tx-log object-store)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (c/new-id picasso)
        business-time #inst "2018-05-21"
        eid (c/new-id :http://dbpedia.org/resource/Pablo_Picasso)
        {:crux.tx/keys [tx-time tx-id]}
        @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso picasso business-time]])
        expected-entities [(c/map->EntityTx {:eid eid
                                               :content-hash content-hash
                                               :bt business-time
                                               :tt tx-time
                                               :tx-id tx-id})]]

    (with-open [snapshot (kv/new-snapshot f/*kv*)]
      (t/testing "can see entity at transact and business time"
        (t/is (= expected-entities
                 (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time tx-time)))
        (t/is (= expected-entities
                 (idx/all-entities snapshot tx-time tx-time))))

      (t/testing "cannot see entity before business or transact time"
        (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" tx-time)))
        (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time #inst "2018-05-20")))

        (t/is (empty? (idx/all-entities snapshot #inst "2018-05-20" tx-time)))
        (t/is (empty? (idx/all-entities snapshot tx-time #inst "2018-05-20"))))

      (t/testing "can see entity after business or transact time"
        (t/is (some? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" tx-time)))
        (t/is (some? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] tx-time tx-time))))

      (t/testing "can see entity history"
        (t/is (= [(c/map->EntityTx {:eid eid
                                      :content-hash content-hash
                                      :bt business-time
                                      :tt tx-time
                                      :tx-id tx-id})]
                 (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (c/new-id new-picasso)
            new-business-time #inst "2018-05-20"
            {new-tx-time :crux.tx/tx-time
             new-tx-id :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/is (= [(c/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-tx-time
                                        :tx-id new-tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time)))
          (t/is (= [(c/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-tx-time
                                        :tx-id new-tx-id})] (idx/all-entities snapshot new-business-time new-tx-time)))

          (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21"))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (c/new-id new-picasso)
            new-business-time #inst "2018-05-22"
            {new-tx-time :crux.tx/tx-time
             new-tx-id :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/is (= [(c/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-tx-time
                                        :tx-id new-tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time)))
          (t/is (= [(c/map->EntityTx {:eid eid
                                        :content-hash content-hash
                                        :bt business-time
                                        :tt tx-time
                                        :tx-id tx-id})]
                   (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time tx-time)))
          (t/is (= [(c/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-tx-time
                                        :tx-id new-tx-id})] (idx/all-entities snapshot new-business-time new-tx-time))))

        (t/testing "can correct entity at earlier business time"
          (let [new-picasso (assoc picasso :bar :foo)
                new-content-hash (c/new-id new-picasso)
                prev-tx-time new-tx-time
                prev-tx-id new-tx-id
                new-business-time #inst "2018-05-22"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id :crux.tx/tx-id}
                @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

            (with-open [snapshot (kv/new-snapshot f/*kv*)]
              (t/is (= [(c/map->EntityTx {:eid eid
                                            :content-hash new-content-hash
                                            :bt new-business-time
                                            :tt new-tx-time
                                            :tx-id new-tx-id})]
                       (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time)))
              (t/is (= [(c/map->EntityTx {:eid eid
                                            :content-hash new-content-hash
                                            :bt new-business-time
                                            :tt new-tx-time
                                            :tx-id new-tx-id})] (idx/all-entities snapshot new-business-time new-tx-time)))

              (t/is (= prev-tx-id (-> (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] prev-tx-time prev-tx-time)
                                      (first)
                                      :tx-id))))

            (t/testing "compare and set does nothing with wrong content hash"
              (let [old-picasso (assoc picasso :baz :boz)
                    {cas-failure-tx-time :crux.tx/tx-time}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])]
                (t/is (= cas-failure-tx-time (tx/await-tx-time indexer cas-failure-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
                  (t/is (= [(c/map->EntityTx {:eid eid
                                                :content-hash new-content-hash
                                                :bt new-business-time
                                                :tt new-tx-time
                                                :tx-id new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time cas-failure-tx-time))))))

            (t/testing "compare and set updates with correct content hash"
              (let [old-picasso new-picasso
                    new-picasso (assoc old-picasso :baz :boz)
                    new-content-hash (c/new-id new-picasso)
                    {new-tx-time :crux.tx/tx-time
                     new-tx-id :crux.tx/tx-id}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])]
                (t/is (= new-tx-time (tx/await-tx-time indexer new-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
                  (t/is (= [(c/map->EntityTx {:eid eid
                                                :content-hash new-content-hash
                                                :bt new-business-time
                                                :tt new-tx-time
                                                :tx-id new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time))))))

            (t/testing "compare and set can update non existing nil entity"
              (let [new-eid (c/new-id :http://dbpedia.org/resource/Pablo2)
                    new-picasso (assoc new-picasso :crux.db/id :http://dbpedia.org/resource/Pablo2)
                    new-content-hash (c/new-id new-picasso)
                    {new-tx-time :crux.tx/tx-time
                     new-tx-id :crux.tx/tx-id}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo2 nil new-picasso new-business-time]])]
                (t/is (= new-tx-time (tx/await-tx-time indexer new-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
                  (t/is (= [(c/map->EntityTx {:eid new-eid
                                                :content-hash new-content-hash
                                                :bt new-business-time
                                                :tt new-tx-time
                                                :tx-id new-tx-id})]
                           (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo2] new-business-time new-tx-time))))))))

        (t/testing "can delete entity"
          (let [new-business-time #inst "2018-05-23"
                {new-tx-time :crux.tx/tx-time
                 new-tx-id :crux.tx/tx-id}
                @(db/submit-tx tx-log [[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso new-business-time]])]
            (with-open [snapshot (kv/new-snapshot f/*kv*)]
              (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time)))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (-> (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] business-time new-tx-time)
                                   (first)
                                   :tx-id)))))))))

    (t/testing "can retrieve history of entity"
      (with-open [snapshot (kv/new-snapshot f/*kv*)]
        (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
          (t/is (= 6 (count (map :content-hash picasso-history))))
          (with-open [i (kv/new-iterator snapshot)]
            (doseq [{:keys [content-hash]} picasso-history
                    :when (not (bu/bytes=? c/nil-id-bytes (c/id->bytes content-hash)))
                    :let [version-k (c/encode-attribute+entity+value+content-hash-key
                                     (c/id->bytes :http://xmlns.com/foaf/0.1/givenName)
                                     (c/id->bytes :http://dbpedia.org/resource/Pablo_Picasso)
                                     (c/value->bytes "Pablo")
                                     (c/id->bytes content-hash))]]
              (t/is (bu/bytes=? version-k (kv/seek (idx/new-prefix-kv-iterator i version-k) version-k))))))))

    (t/testing "can evict entity"
      (let [new-business-time #inst "2018-05-23"
            {new-tx-time :crux.tx/tx-time
             new-tx-id :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso #inst "1970" new-business-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-tx-time)))

          (t/testing "eviction adds to and keeps tx history"
            (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
              (t/is (= 7 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (db/get-objects object-store snapshot (keep :content-hash picasso-history)))))
              (t/testing "eviction removes secondary indexes"
                (with-open [i (kv/new-iterator snapshot)]
                  (doseq [{:keys [content-hash]} picasso-history
                          :let [version-k (c/encode-attribute+entity+value+content-hash-key
                                           (c/id->bytes :http://xmlns.com/foaf/0.1/givenName)
                                           (c/id->bytes :http://dbpedia.org/resource/Pablo_Picasso)
                                           (c/value->bytes "Pablo")
                                           (c/id->bytes content-hash))]]
                    (t/is (nil? (kv/seek (idx/new-prefix-kv-iterator i version-k) version-k)))))))))))))

(t/deftest test-can-correct-ranges-in-the-past
  (let [tx-log (tx/->KvTxLog f/*kv*)
        object-store (idx/->KvObjectStore f/*kv*)
        ivan {:crux.db/id :ivan :name "Ivan"}

        v1-ivan (assoc ivan :version 1)
        v1-business-time #inst "2018-11-26"
        {v1-tx-time :crux.tx/tx-time
         v1-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v1-ivan v1-business-time]])

        v2-ivan (assoc ivan :version 2)
        v2-business-time #inst "2018-11-27"
        {v2-tx-time :crux.tx/tx-time
         v2-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v2-ivan v2-business-time]])

        v3-ivan (assoc ivan :version 3)
        v3-business-time #inst "2018-11-28"
        {v3-tx-time :crux.tx/tx-time
         v3-tx-id :crux.tx/tx-id}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan v3-ivan v3-business-time]])]

    (with-open [snapshot (kv/new-snapshot f/*kv*)]
      (t/testing "first version of entity is visible"
        (t/is (= v1-tx-id (-> (idx/entities-at snapshot [:ivan] v1-business-time v3-tx-time)
                              (first)
                              :tx-id))))

      (t/testing "second version of entity is visible"
        (t/is (= v2-tx-id (-> (idx/entities-at snapshot [:ivan] v2-business-time v3-tx-time)
                              (first)
                              :tx-id))))

      (t/testing "third version of entity is visible"
        (t/is (= v3-tx-id (-> (idx/entities-at snapshot [:ivan] v3-business-time v3-tx-time)
                              (first)
                              :tx-id)))))

    (let [corrected-ivan (assoc ivan :version 4)
          corrected-start-business-time #inst "2018-11-27"
          corrected-end-business-time #inst "2018-11-29"
          {corrected-tx-time :crux.tx/tx-time
           corrected-tx-id :crux.tx/tx-id}
          @(db/submit-tx tx-log [[:crux.tx/put :ivan corrected-ivan corrected-start-business-time corrected-end-business-time]])]

      (with-open [snapshot (kv/new-snapshot f/*kv*)]
        (t/testing "first version of entity is still there"
          (t/is (= v1-tx-id (-> (idx/entities-at snapshot [:ivan] v1-business-time corrected-tx-time)
                                (first)
                                :tx-id))))

        (t/testing "second version of entity was corrected"
          (t/is (= {:content-hash (c/new-id corrected-ivan)
                    :tx-id corrected-tx-id}
                   (-> (idx/entities-at snapshot [:ivan] v2-business-time corrected-tx-time)
                       (first)
                       (select-keys [:tx-id :content-hash])))))

        (t/testing "third version of entity was corrected"
          (t/is (= {:content-hash (c/new-id corrected-ivan)
                    :tx-id corrected-tx-id}
                   (-> (idx/entities-at snapshot [:ivan] v3-business-time corrected-tx-time)
                       (first)
                       (select-keys [:tx-id :content-hash]))))))

      (let [deleted-start-business-time #inst "2018-11-25"
            deleted-end-business-time #inst "2018-11-28"
            {deleted-tx-time :crux.tx/tx-time
             deleted-tx-id :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/delete :ivan deleted-start-business-time deleted-end-business-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/testing "first version of entity was deleted"
            (t/is (empty? (idx/entities-at snapshot [:ivan] v1-business-time deleted-tx-time))))

          (t/testing "second version of entity was deleted"
            (t/is (empty? (idx/entities-at snapshot [:ivan] v2-business-time deleted-tx-time))))

          (t/testing "third version of entity is still there"
            (t/is (= {:content-hash (c/new-id corrected-ivan)
                      :tx-id corrected-tx-id}
                     (-> (idx/entities-at snapshot [:ivan] v3-business-time deleted-tx-time)
                         (first)
                         (select-keys [:tx-id :content-hash])))))))

      (t/testing "end range is exclusive"
        (let [{deleted-tx-time :crux.tx/tx-time
               deleted-tx-id :crux.tx/tx-id}
              @(db/submit-tx tx-log [[:crux.tx/delete :ivan v3-business-time v3-business-time]])]

          (with-open [snapshot (kv/new-snapshot f/*kv*)]
            (t/testing "third version of entity is still there"
              (t/is (= {:content-hash (c/new-id corrected-ivan)
                        :tx-id corrected-tx-id}
                       (-> (idx/entities-at snapshot [:ivan] v3-business-time deleted-tx-time)
                           (first)
                           (select-keys [:tx-id :content-hash])))))))))))

(t/deftest test-can-read-kv-tx-log
  (let [tx-log (tx/->KvTxLog f/*kv*)
        ivan {:crux.db/id :ivan :name "Ivan"}

        tx1-ivan (assoc ivan :version 1)
        tx1-business-time #inst "2018-11-26"
        {tx1-id :crux.tx/tx-id
         tx1-tx-time :crux.tx/tx-time}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan tx1-ivan tx1-business-time]])

        tx2-ivan (assoc ivan :version 2)
        tx2-petr {:crux.db/id :petr :name "Petr"}
        tx2-business-time #inst "2018-11-27"
        {tx2-id :crux.tx/tx-id
         tx2-tx-time :crux.tx/tx-time}
        @(db/submit-tx tx-log [[:crux.tx/put :ivan tx2-ivan tx2-business-time]
                               [:crux.tx/put :petr tx2-petr tx2-business-time]])]

    (with-open [tx-log-context (db/new-tx-log-context tx-log)]
      (let [log (db/tx-log tx-log tx-log-context)]
        (t/is (not (realized? log)))
        (t/is (= [{:crux.tx/tx-id tx1-id
                   :crux.tx/tx-time tx1-tx-time
                   :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id tx1-ivan) tx1-business-time]]}
                  {:crux.tx/tx-id tx2-id
                   :crux.tx/tx-time tx2-tx-time
                   :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id tx2-ivan) tx2-business-time]
                                    [:crux.tx/put (c/new-id :petr) (c/new-id tx2-petr) tx2-business-time]]}]
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
             [[(c/value->bytes 1) :a]
              [(c/value->bytes 3) :c]])]
    (t/is (= :a
             (second (db/seek-values idx (c/value->bytes 0)))))
    (t/is (= :a
             (second (db/seek-values idx (c/value->bytes 1)))))
    (t/is (= :c
             (second (db/next-values idx))))
    (t/is (= :c
             (second (db/seek-values idx (c/value->bytes 2)))))
    (t/is (= :c
             (second (db/seek-values idx (c/value->bytes 3)))))
    (t/is (nil? (db/seek-values idx (c/value->bytes 4))))))

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
      (t/is (= 2 (second (db/seek-values (idx/new-greater-than-equal-virtual-index r 2) (c/value->bytes nil)))))
      (t/is (= 3 (second (db/seek-values (idx/new-greater-than-virtual-index r 2) (c/value->bytes 1))))))

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
               [[(c/value->bytes 1) :a]
                [(c/value->bytes 3) :c]
                [(c/value->bytes 5) :e1]])
        idx-2 (idx/new-sorted-virtual-index
               [[(c/value->bytes 2) :b]
                [(c/value->bytes 4) :d]
                [(c/value->bytes 5) :e2]
                [(c/value->bytes 7) :g]])
        idx-3 (idx/new-sorted-virtual-index
               [[(c/value->bytes 5) :e3]
                [(c/value->bytes 6) :f]])
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
               (second (db/seek-values idx (c/value->bytes 4)))))
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
