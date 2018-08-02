(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [crux.byte-utils :as bu]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.tx :as tx]
            [crux.kv-store :as ks]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.fixtures :as f]
            [taoensso.nippy :as nippy])
  (:import [java.util Date]
           [java.nio ByteBuffer]))

(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-id %)))))

(t/deftest test-can-store-doc
  (let [tx-log (tx/->DocTxLog f/*kv*)
        object-store (doc/->DocObjectStore f/*kv*)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)]
    (t/is (= 48 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (db/submit-doc tx-log content-hash picasso)
    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/is (= {content-hash picasso}
               (db/get-objects object-store snapshot [content-hash])))

      (t/testing "non existent docs are ignored"
        (t/is (= {content-hash picasso}
                 (db/get-objects object-store
                                 snapshot
                                 [content-hash
                                  "090622a35d4b579d2fcfebf823821298711d3867"])))
        (t/is (empty? (db/get-objects object-store snapshot [])))))))

(t/deftest test-can-index-tx-ops
  (let [tx-log (tx/->DocTxLog f/*kv*)
        object-store (doc/->DocObjectStore f/*kv*)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)
        business-time #inst "2018-05-21"
        eid (idx/new-id :http://dbpedia.org/resource/Pablo_Picasso)
        {:keys [transact-time tx-id]}
        @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso picasso business-time]])
        expected-entities [(idx/map->EntityTx {:eid eid
                                               :content-hash content-hash
                                               :bt business-time
                                               :tt transact-time
                                               :tx-id tx-id})]]

    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/testing "can see entity at transact and business time"
        (t/is (= expected-entities
                 (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time)))
        (t/is (= expected-entities
                 (doc/all-entities snapshot transact-time transact-time))))

      (t/testing "cannot see entity before business or transact time"
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-20")))

        (t/is (empty? (doc/all-entities snapshot #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/all-entities snapshot transact-time #inst "2018-05-20"))))

      (t/testing "can see entity after business or transact time"
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" transact-time)))
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time))))

      (t/testing "can see entity history"
        (t/is (= [(idx/map->EntityTx {:eid eid
                                      :content-hash content-hash
                                      :bt business-time
                                      :tt transact-time
                                      :tx-id tx-id})]
                 (doc/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-20"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= [(idx/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-transact-time
                                        :tx-id new-tx-id})]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= [(idx/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-transact-time
                                        :tx-id new-tx-id})] (doc/all-entities snapshot new-business-time new-transact-time)))

          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21"))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-22"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= [(idx/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-transact-time
                                        :tx-id new-tx-id})]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= [(idx/map->EntityTx {:eid eid
                                        :content-hash content-hash
                                        :bt business-time
                                        :tt transact-time
                                        :tx-id tx-id})]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time transact-time)))
          (t/is (= [(idx/map->EntityTx {:eid eid
                                        :content-hash new-content-hash
                                        :bt new-business-time
                                        :tt new-transact-time
                                        :tx-id new-tx-id})] (doc/all-entities snapshot new-business-time new-transact-time))))

        (t/testing "can correct entity at earlier business time"
          (let [new-picasso (assoc picasso :bar :foo)
                new-content-hash (idx/new-id new-picasso)
                prev-transact-time new-transact-time
                prev-tx-id new-tx-id
                new-business-time #inst "2018-05-22"
                {new-transact-time :transact-time
                 new-tx-id :tx-id}
                @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

            (with-open [snapshot (ks/new-snapshot f/*kv*)]
              (t/is (= [(idx/map->EntityTx {:eid eid
                                            :content-hash new-content-hash
                                            :bt new-business-time
                                            :tt new-transact-time
                                            :tx-id new-tx-id})]
                       (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
              (t/is (= [(idx/map->EntityTx {:eid eid
                                            :content-hash new-content-hash
                                            :bt new-business-time
                                            :tt new-transact-time
                                            :tx-id new-tx-id})] (doc/all-entities snapshot new-business-time new-transact-time)))

              (t/is (= prev-tx-id (-> (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] prev-transact-time prev-transact-time)
                                      (first)
                                      :tx-id))))

            (t/testing "compare and set does nothing with wrong content hash"
              (let [old-picasso (assoc picasso :baz :boz)]
                @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= [(idx/map->EntityTx {:eid eid
                                                :content-hash new-content-hash
                                                :bt new-business-time
                                                :tt new-transact-time
                                                :tx-id new-tx-id})]
                           (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time))))))

            (t/testing "compare and set updates with correct content hash"
              (let [old-picasso new-picasso
                    new-picasso (assoc old-picasso :baz :boz)
                    new-content-hash (idx/new-id new-picasso)
                    {new-transact-time :transact-time
                     new-tx-id :tx-id}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])]
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= [(idx/map->EntityTx {:eid eid
                                                :content-hash new-content-hash
                                                :bt new-business-time
                                                :tt new-transact-time
                                                :tx-id new-tx-id})]
                           (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time))))))))

        (t/testing "can delete entity"
          (let [new-business-time #inst "2018-05-23"
                {new-transact-time :transact-time
                 new-tx-id :tx-id}
                @(db/submit-tx tx-log [[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso new-business-time]])]
            (with-open [snapshot (ks/new-snapshot f/*kv*)]
              (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (-> (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] business-time new-transact-time)
                                   (first)
                                   :tx-id)))))))))

    (t/testing "can retrieve history of entity"
      (with-open [snapshot (ks/new-snapshot f/*kv*)]
        (let [picasso-history (doc/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
          (t/is (= 6 (count (map :content-hash picasso-history))))
          (with-open [i (ks/new-iterator snapshot)]
            (doseq [{:keys [content-hash]} picasso-history
                    :when (not (bu/bytes=? idx/nil-id-bytes (idx/id->bytes content-hash)))
                    :let [version-k (idx/encode-attribute+entity+value+content-hash-key
                                     (idx/id->bytes :http://xmlns.com/foaf/0.1/givenName)
                                     (idx/id->bytes :http://dbpedia.org/resource/Pablo_Picasso)
                                     (idx/value->bytes "Pablo")
                                     (idx/id->bytes content-hash))]]
              (t/is (bu/bytes=? version-k (ks/seek (doc/new-prefix-kv-iterator i version-k) version-k))))))))

    (t/testing "can evict entity"
      (let [new-business-time #inst "2018-05-23"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))

          (t/testing "eviction adds to and keeps tx history"
            (let [picasso-history (doc/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
              (t/is (= 7 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (db/get-objects object-store snapshot (keep :content-hash picasso-history)))))
              (t/testing "eviction removes secondary indexes"
                (with-open [i (ks/new-iterator snapshot)]
                  (doseq [{:keys [content-hash]} picasso-history
                          :let [version-k (idx/encode-attribute+entity+value+content-hash-key
                                           (idx/id->bytes :http://xmlns.com/foaf/0.1/givenName)
                                           (idx/id->bytes :http://dbpedia.org/resource/Pablo_Picasso)
                                           (idx/value->bytes "Pablo")
                                           (idx/id->bytes content-hash))]]
                    (t/is (nil? (ks/seek (doc/new-prefix-kv-iterator i version-k) version-k)))))))))))))

(t/deftest test-can-perform-unary-join
  (let [a-idx (doc/new-relation-virtual-index :a
                                              [[0]
                                               [1]
                                               [3]
                                               [4]
                                               [5]
                                               [6]
                                               [7]
                                               [8]
                                               [8]
                                               [9]
                                               [11]
                                               [12]]
                                              1)
        b-idx (doc/new-relation-virtual-index :b
                                              [[0]
                                               [2]
                                               [6]
                                               [7]
                                               [8]
                                               [9]
                                               [12]]
                                              1)
        c-idx (doc/new-relation-virtual-index :c
                                              [[2]
                                               [4]
                                               [5]
                                               [8]
                                               [10]
                                               [12]]
                                              1)]

    (t/is (= [{:x #{8}}
              {:x #{12}}]
             (for [[_ join-results] (-> (doc/new-unary-join-virtual-index [(assoc a-idx :name :x)
                                                                           (assoc b-idx :name :x)
                                                                           (assoc c-idx :name :x)])
                                        (doc/idx->seq))]
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
  (let [r (doc/new-relation-virtual-index :r
                                          [[1 3]
                                           [1 4]
                                           [1 5]
                                           [3 5]]
                                          2)
        s (doc/new-relation-virtual-index :s
                                          [[3 4]
                                           [3 5]
                                           [4 6]
                                           [4 8]
                                           [4 9]
                                           [5 2]]
                                          2)
        t (doc/new-relation-virtual-index :t
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
            result (-> (mapv doc/new-unary-join-virtual-index index-groups)
                       (doc/new-n-ary-join-layered-virtual-index)
                       (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                       (doc/layered-idx->seq))]
        (t/is (= [{:a #{1}, :b #{3}, :c #{4}}
                  {:a #{1}, :b #{3}, :c #{5}}
                  {:a #{1}, :b #{4}, :c #{6}}
                  {:a #{1}, :b #{4}, :c #{8}}
                  {:a #{1}, :b #{4}, :c #{9}}
                  {:a #{1}, :b #{5}, :c #{2}}
                  {:a #{3}, :b #{5}, :c #{2}}]
                 (for [[_ join-results] result]
                   join-results)))))))

(t/deftest test-sorted-virtual-index
  (let [idx (doc/new-sorted-virtual-index
             [[(idx/value->bytes 1) :a]
              [(idx/value->bytes 3) :c]])]
    (t/is (= :a
             (second (db/seek-values idx (idx/value->bytes 0)))))
    (t/is (= :a
             (second (db/seek-values idx (idx/value->bytes 1)))))
    (t/is (= :c
             (second (db/next-values idx))))
    (t/is (= :c
             (second (db/seek-values idx (idx/value->bytes 2)))))
    (t/is (= :c
             (second (db/seek-values idx (idx/value->bytes 3)))))
    (t/is (nil? (db/seek-values idx (idx/value->bytes 4))))))

(t/deftest test-or-virtual-index
  (let [idx-1 (doc/new-sorted-virtual-index
               [[(idx/value->bytes 1) :a]
                [(idx/value->bytes 3) :c]
                [(idx/value->bytes 5) :e1]])
        idx-2 (doc/new-sorted-virtual-index
               [[(idx/value->bytes 2) :b]
                [(idx/value->bytes 4) :d]
                [(idx/value->bytes 5) :e2]
                [(idx/value->bytes 7) :g]])
        idx-3 (doc/new-sorted-virtual-index
               [[(idx/value->bytes 5) :e3]
                [(idx/value->bytes 6) :f]])
        idx (doc/new-or-virtual-index [idx-1 idx-2 idx-3])]
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
               (second (db/seek-values idx (idx/value->bytes 4)))))
      (t/is (= :e1
               (second (db/next-values idx)))))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (doc/read-meta f/*kv* :foo)))
  (doc/store-meta f/*kv* :foo {:bar 2})
  (t/is (= {:bar 2} (doc/read-meta f/*kv* :foo))))

;; NOTE: variable order must align up with relation position order
;; here. This implies that a relation cannot use the same variable
;; twice in two positions. All relations and the join order must be in
;; the same order for it to work.
(t/deftest test-n-ary-join-based-on-relational-tuples
  (let [r-idx (doc/new-relation-virtual-index :r
                                              [[7 4]
                                               ;; extra sanity check
                                               [8 4]]
                                              2)
        s-idx (doc/new-relation-virtual-index :s
                                              [[4 0]
                                               [4 1]
                                               [4 2]
                                               [4 3]]
                                              2)
        t-idx (doc/new-relation-virtual-index :t
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
             (set (for [[_ join-results] (-> (mapv doc/new-unary-join-virtual-index index-groups)
                                             (doc/new-n-ary-join-layered-virtual-index)
                                             (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                             (doc/layered-idx->seq))
                        result (#'crux.query/cartesian-product
                                (for [var [:a :b :c]]
                                  (get join-results var)))]
                    (vec result)))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-unary-conjunction-and-disjunction
  (let [p-idx (doc/new-relation-virtual-index :p
                                              [[1]
                                               [2]
                                               [3]]
                                              1)
        q-idx (doc/new-relation-virtual-index :q
                                              [[2]
                                               [3]
                                               [4]]
                                              1)
        r-idx (doc/new-relation-virtual-index :r
                                              [[3]
                                               [4]
                                               [5]]
                                              1)]
    (t/testing "conjunction"
      (let [unary-and-idx (doc/new-unary-join-virtual-index [(assoc p-idx :name :x)
                                                             (assoc q-idx :name :x)
                                                             (assoc r-idx :name :x)])]
        (t/is (= #{[3]}
                 (set (for [[_ join-results] (-> (doc/new-n-ary-join-layered-virtual-index [unary-and-idx])
                                                 (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                 (doc/layered-idx->seq))
                            result (#'crux.query/cartesian-product
                                    (for [var [:x]]
                                      (get join-results var)))]
                        (vec result)))))))

    (t/testing "disjunction"
      (let [unary-or-idx (doc/new-or-virtual-index
                          [(doc/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                           (doc/new-unary-join-virtual-index [(assoc q-idx :name :x)
                                                              (assoc r-idx :name :x)])])]
        (t/is (= #{[1]
                   [2]
                   [3]
                   [4]}
                 (set (for [[_ join-results] (-> (doc/new-n-ary-join-layered-virtual-index [unary-or-idx])
                                                 (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                 (doc/layered-idx->seq))
                            result (#'crux.query/cartesian-product
                                    (for [var [:x]]
                                      (get join-results var)))]
                        (vec result)))))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-n-ary-conjunction-and-disjunction
  (let [p-idx (doc/new-relation-virtual-index :p
                                              [[1 3]
                                               [2 4]
                                               [2 20]]
                                              2)
        q-idx (doc/new-relation-virtual-index :q
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
               (set (for [[_ join-results] (-> (mapv doc/new-unary-join-virtual-index index-groups)
                                               (doc/new-n-ary-join-layered-virtual-index)
                                               (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                               (doc/layered-idx->seq))
                          result (#'crux.query/cartesian-product
                                  (for [var [:x :y :z]]
                                    (get join-results var)))]
                      (vec result))))))

    (t/testing "disjunction"
      (let [zero-idx (doc/new-relation-virtual-index :zero
                                                     [[0]]
                                                     1)
            lhs-index (doc/new-n-ary-join-layered-virtual-index
                       [(doc/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                        (doc/new-unary-join-virtual-index [(assoc p-idx :name :y)])
                        (doc/new-unary-join-virtual-index [(assoc zero-idx :name :z)])])]
        (t/is (= #{[1 3 0]
                   [2 4 0]
                   [2 20 0]}
                 (set (for [[_ join-results] (-> lhs-index
                                                 (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                 (doc/layered-idx->seq))
                            result (#'crux.query/cartesian-product
                                    (for [var [:x :y :z]]
                                      (get join-results var)))]
                        (vec result)))))
        (let [rhs-index (doc/new-n-ary-join-layered-virtual-index
                         [(doc/new-unary-join-virtual-index [(assoc q-idx :name :x)])
                          (doc/new-unary-join-virtual-index [(assoc zero-idx :name :y)])
                          (doc/new-unary-join-virtual-index [(assoc q-idx :name :z)])])]
          (t/is (= #{[1 0 10]
                     [2 0 20]
                     [3 0 30]}
                   (set (for [[_ join-results] (-> rhs-index
                                                   (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                   (doc/layered-idx->seq))
                              result (#'crux.query/cartesian-product
                                      (for [var [:x :y :z]]
                                        (get join-results var)))]
                          (vec result)))))
          ;; TODO: Should we need to specify the vars? The lhs and rhs
          ;; vars needs to be the same.
          (let [n-ary-or (doc/new-n-ary-or-layered-virtual-index lhs-index rhs-index)]
            (t/is (= #{[1 3 0]
                       [2 4 0]
                       [1 0 10]
                       [2 0 20]
                       [2 20 0]
                       [3 0 30]}
                     (set (for [[_ join-results] (-> n-ary-or
                                                     (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                     (doc/layered-idx->seq))
                                result (#'crux.query/cartesian-product
                                        (for [var [:x :y :z]]
                                          (get join-results var)))]
                            (vec result)))))))))))
