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
    (t/is (= {content-hash picasso}
             (db/get-objects object-store [content-hash])))

    (t/testing "non existent docs are ignored"
      (t/is (= {content-hash picasso}
               (db/get-objects object-store
                               [content-hash
                                "090622a35d4b579d2fcfebf823821298711d3867"])))
      (t/is (empty? (db/get-objects object-store []))))))

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

      #_(t/testing "can find entity by secondary index"
          (t/testing "single value attribute")
          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at snapshot :http://xmlns.com/foaf/0.1/givenName
                                                       #(doc/new-equal-virtual-index % "Pablo")
                                                       transact-time transact-time)))

          (t/testing "find multi valued attribute"
            (t/is (= expected-entities
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://purl.org/dc/terms/subject
                      #(doc/new-equal-virtual-index % :http://dbpedia.org/resource/Category:Cubist_artists)
                      transact-time transact-time))))

          (t/testing "find attribute by range"
            (t/is (= expected-entities
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(doc/new-equal-virtual-index % 230)
                      transact-time transact-time)))
            (t/is (= expected-entities
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 229)
                           (doc/new-less-than-equal-virtual-index 230))
                      transact-time transact-time)))
            (t/is (= expected-entities
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 229)
                           (doc/new-less-than-equal-virtual-index 231))
                      transact-time transact-time)))
            (t/is (= expected-entities
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 230)
                           (doc/new-less-than-equal-virtual-index 231))
                      transact-time transact-time)))

            (t/testing "not inclusive operator"
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-virtual-index 230)
                           (doc/new-less-than-equal-virtual-index 231))
                      transact-time transact-time)))
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 229)
                           (doc/new-less-than-virtual-index 230))
                      transact-time transact-time)))
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-virtual-index 230)
                           (doc/new-less-than-virtual-index 230))
                      transact-time transact-time))))

            (t/testing "not within range"
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 231)
                           (doc/new-less-than-equal-virtual-index 255))
                      transact-time transact-time)))
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index 1)
                           (doc/new-less-than-equal-virtual-index 229))
                      transact-time transact-time)))
              (t/is (empty?
                     (doc/entities-by-attribute-value-at
                      snapshot
                      :http://dbpedia.org/property/imageSize
                      #(-> %
                           (doc/new-greater-than-equal-virtual-index -255)
                           (doc/new-less-than-equal-virtual-index 229))
                      transact-time transact-time))))))

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
          (t/is (= 6 (count (map :content-hash picasso-history)))))))

    (t/testing "can evict entity"
      (let [new-business-time #inst "2018-05-23"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))

          (t/testing "eviction adds to and keeps tx history"
            (let [picasso-history (doc/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
              ;; TODO: this is flaky
              ;; (t/is (= 7 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (db/get-objects object-store (keep :content-hash picasso-history)))))))

          ;; TODO: this functionality not exposed anymore. Could check
          ;; index directly.
          #_(t/testing "eviction removes secondary indexes"
              (t/is (empty? (doc/entities-by-attribute-value-at snapshot :http://xmlns.com/foaf/0.1/givenName
                                                                #(doc/new-equal-virtual-index % "Pablo")
                                                                new-transact-time new-transact-time)))))))))

;; TODO: These commented out tests don't work with the binary index,
;; are indirectly covered by query tests. Are possible to rewrite to
;; work in the new world, or to port to normal query-level testsl
#_(t/deftest test-can-perform-unary-join
  (let [tx-log (tx/->DocTxLog f/*kv*)
        tx-ops (vec (concat (for [[relation vs] {:a [0 1 3 4 5 6 7 8 8 9 11 12]
                                                 :b [0 2 6 7 8 9 12 12]
                                                 :c [2 4 5 8 10 12 12]}
                                  [i v] (map-indexed vector vs)
                                  :let [eid (keyword (str (name relation) i "-" v))]]
                              [:crux.tx/put eid {:crux.db/id eid relation v}])))
        {:keys [transact-time tx-id]}
        @(db/submit-tx tx-log tx-ops)]
    (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot f/*kv*) true)]
      (t/testing "checking data is loaded before join"
        (t/is (= (idx/new-id :a0-0)
                 (:eid (first (doc/entities-at snapshot [:a0-0] transact-time transact-time)))))
        (t/is (= (count tx-ops) (count (doc/all-entities snapshot transact-time transact-time)))))

      (t/testing "unary join"
        (let [a-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :a nil transact-time transact-time)
                        (assoc :name :a))
              b-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :b nil transact-time transact-time)
                        (assoc :name :b))
              c-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :c nil transact-time transact-time)
                        (assoc :name :c))]
          (t/is (= [{:a #{(idx/new-id :a7-8)
                          (idx/new-id :a8-8)}
                     :b #{(idx/new-id :b4-8)}
                     :c #{(idx/new-id :c3-8)}}
                    {:a #{(idx/new-id :a11-12)}
                     :b #{(idx/new-id :b6-12)
                          (idx/new-id :b7-12)}
                     :c #{(idx/new-id :c5-12)
                          (idx/new-id :c6-12)}}]
                   (for [[v join-results] (->> (doc/new-unary-join-virtual-index [a-idx b-idx c-idx])
                                               (doc/idx->seq))]
                     (->> (for [[k entities] join-results]
                            [k (set (map :eid entities))])
                          (into {}))))))))))

;; Q(a, b, c) ← R(a, b), S(b, c), T (a, c).

;; (1, 3, 4)
;; (1, 3, 5)
;; (1, 4, 6)
;; (1, 4, 8)
;; (1, 4, 9)
;; (1, 5, 2)
;; (3, 5, 2)
;; TODO: Same as above.
#_(t/deftest test-can-perform-n-ary-join
  (let [data [{:crux.db/id :r13 :ra 1 :rb 3}
              {:crux.db/id :r14 :ra 1 :rb 4}
              {:crux.db/id :r15 :ra 1 :rb 5}
              {:crux.db/id :r35 :ra 3 :rb 5}
              {:crux.db/id :s34 :sb 3 :sc 4}
              {:crux.db/id :s35 :sb 3 :sc 5}
              {:crux.db/id :s46 :sb 4 :sc 6}
              {:crux.db/id :s48 :sb 4 :sc 8}
              {:crux.db/id :s49 :sb 4 :sc 9}
              {:crux.db/id :s52 :sb 5 :sc 2}
              {:crux.db/id :t14 :ta 1 :tc 4}
              {:crux.db/id :t15 :ta 1 :tc 5}
              {:crux.db/id :t16 :ta 1 :tc 6}
              {:crux.db/id :t18 :ta 1 :tc 8}
              {:crux.db/id :t19 :ta 1 :tc 9}
              {:crux.db/id :t12 :ta 1 :tc 2}
              {:crux.db/id :t32 :ta 3 :tc 2}]]
    (let [tx-log (tx/->DocTxLog f/*kv*)
          tx-ops (vec (concat (for [{:keys [crux.db/id] :as doc} data]
                                [:crux.tx/put id doc])))
          {:keys [transact-time tx-id]}
          @(db/submit-tx tx-log tx-ops)]
      (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot f/*kv*) true)]
        (t/testing "checking data is loaded before join"
          (t/is (= (count tx-ops)
                   (count (doc/all-entities snapshot transact-time transact-time)))))

        (t/testing "n-ary join"
          (let [ra-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ra nil transact-time transact-time)
                           (assoc :name :r))
                ta-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ta nil transact-time transact-time)
                           (assoc :name :t))
                rb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :rb nil transact-time transact-time)
                           (assoc :name :r))
                sb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sb nil transact-time transact-time)
                           (assoc :name :s))
                sc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sc nil transact-time transact-time)
                           (assoc :name :s))
                tc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :tc nil transact-time transact-time)
                           (assoc :name :t))
                index-groups [[ra-idx ta-idx]
                              [rb-idx sb-idx]
                              [sc-idx tc-idx]]
                result (-> (mapv doc/new-unary-join-virtual-index index-groups)
                           (doc/new-n-ary-join-layered-virtual-index)
                           (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                           (doc/layered-idx->seq))]
            (t/testing "order of results"
              (t/is (= (vec (for [[a b c] [[1 3 4]
                                           [1 3 5]
                                           [1 4 6]
                                           [1 4 8]
                                           [1 4 9]
                                           [1 5 2]
                                           [3 5 2]]]
                              [(bu/bytes->hex (idx/value->bytes a))
                               (bu/bytes->hex (idx/value->bytes b))
                               (bu/bytes->hex (idx/value->bytes c))]))
                       (vec (for [[[a b c] join-results] result]
                              [(bu/bytes->hex a)
                               (bu/bytes->hex b)
                               (bu/bytes->hex c)])))))
            (t/is (= (set (map (comp idx/new-id :crux.db/id) data))
                     (set (for [[v join-results] result
                                [k entities] join-results
                                {:keys [eid]} entities]
                            eid))))))))))

;; TODO: Same as above.
#_(t/deftest test-n-ary-join-prunes-values-based-on-later-joins
  (let [data [ ;; d365d8e84bb127ed8f4d076f7528641a7ce08049
              {:crux.db/id :r13 :ra 1 :rb 3}
              ;; Unifies with :ta, but not with :sb
              ;; 597d68237e345bbb91eae7751e60a07fb904c8dd
              {:crux.db/id :r14 :ra 1 :rb 4}
              ;; Does not unify with :ta or :sb.
              {:crux.db/id :r25 :ra 2 :rb 5}
              ;; 9434448654674927dbc44b2280d44f92166ac350
              {:crux.db/id :s34 :sb 3 :sc 4}
              ;; Unifies with :rb, but not with :tc
              ;; b824a31f61bf0fc0b498aa038dd9ae5bd08adb64
              {:crux.db/id :s37 :sb 3 :sc 7}
              ;; eed43fbbc28c9b627a8b3e0fba770bab9d7a9465
              {:crux.db/id :t14 :ta 1 :tc 4}
              ;; Unifies with :ra, but not with :sc
              ;; 6c63a4086ad403653314c2ab546aadd54fff897d
              {:crux.db/id :t15 :ta 1 :tc 5}
              ;; Unifies with :sc, but not with :ra
              ;; 41c3f3e9370cc85a4fea723d35b7327d33067c6e
              {:crux.db/id :t34 :ta 3 :tc 4}]]
    (let [tx-log (tx/->DocTxLog f/*kv*)
          tx-ops (vec (concat (for [{:keys [crux.db/id] :as doc} data]
                                [:crux.tx/put id doc])))
          {:keys [transact-time tx-id]}
          @(db/submit-tx tx-log tx-ops)]
      (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot f/*kv*) true)]
        (let [ra-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ra nil transact-time transact-time)
                         (assoc :name :r))
              ta-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ta nil transact-time transact-time)
                         (assoc :name :t))
              rb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :rb nil transact-time transact-time)
                         (assoc :name :r))
              sb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sb nil transact-time transact-time)
                         (assoc :name :s))
              sc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sc nil transact-time transact-time)
                         (assoc :name :s))
              tc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :tc nil transact-time transact-time)
                         (assoc :name :t))
              index-groups [[ra-idx ta-idx]
                            [rb-idx sb-idx]
                            [sc-idx tc-idx]]]
          (t/is (= #{(idx/new-id :r13)
                     (idx/new-id :s34)
                     (idx/new-id :t14)}
                   (set (for [[v join-results] (-> (mapv doc/new-unary-join-virtual-index index-groups)
                                                   (doc/new-n-ary-join-layered-virtual-index)
                                                   (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                   (doc/layered-idx->seq))
                              [k entities] join-results
                              {:keys [eid]} entities]
                          eid)))))))))

(t/deftest test-sorted-virtual-index
  (let [idx (doc/new-sorted-virtual-index
             [[(idx/value->bytes 1) :a]
              [(idx/value->bytes 3) :c]])]
    (t/is (= :a
             (second (db/seek-values idx 0))))
    (t/is (= :a
             (second (db/seek-values idx 1))))
    (t/is (= :c
             (second (db/next-values idx))))
    (t/is (= :c
             (second (db/seek-values idx 2))))
    (t/is (= :c
             (second (db/seek-values idx 3))))
    (t/is (nil? (db/seek-values idx 4)))))

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
               (second (db/seek-values idx 4))))
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
                    (vec result)))))

    ;; TODO: Same as above.
    #_(t/testing "same join with kv indexes"
        (let [data [{:crux.db/id :r74 :ra 7 :rb 4}
                    {:crux.db/id :s40 :sb 4 :sc 0}
                    {:crux.db/id :s41 :sb 4 :sc 1}
                    {:crux.db/id :s42 :sb 4 :sc 2}
                    {:crux.db/id :s43 :sb 4 :sc 3}
                    {:crux.db/id :t70 :ta 7 :tc 0}
                    {:crux.db/id :t71 :ta 7 :tc 1}
                    {:crux.db/id :t72 :ta 7 :tc 2}]]
          (let [tx-log (tx/->DocTxLog f/*kv*)
                tx-ops (vec (concat (for [{:keys [crux.db/id] :as doc} data]
                                      [:crux.tx/put id doc])))
                {:keys [transact-time tx-id]}
                @(db/submit-tx tx-log tx-ops)]
            (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot f/*kv*) true)]
              (let [ra-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ra nil transact-time transact-time)
                               (assoc :name :r))
                    ta-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :ta nil transact-time transact-time)
                               (assoc :name :t))
                    rb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :rb nil transact-time transact-time)
                               (assoc :name :r))
                    sb-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sb nil transact-time transact-time)
                               (assoc :name :s))
                    sc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :sc nil transact-time transact-time)
                               (assoc :name :s))
                    tc-idx (-> (doc/new-entity-attribute-value-virtual-index snapshot :tc nil transact-time transact-time)
                               (assoc :name :t))
                    index-groups [[ra-idx ta-idx]
                                  [rb-idx sb-idx]
                                  [sc-idx tc-idx]]]
                (t/is (= #{(idx/new-id :r74)
                           (idx/new-id :s40)
                           (idx/new-id :s41)
                           (idx/new-id :s42)
                           (idx/new-id :t70)
                           (idx/new-id :t71)
                           (idx/new-id :t72)}
                         (set (for [[v join-results] (-> (mapv doc/new-unary-join-virtual-index index-groups)
                                                         (doc/new-n-ary-join-layered-virtual-index)
                                                         (doc/new-n-ary-constraining-layered-virtual-index doc/constrain-join-result-by-empty-names)
                                                         (doc/layered-idx->seq))
                                    [k entities] join-results
                                    {:keys [eid]} entities]
                                eid)))))))))))

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
