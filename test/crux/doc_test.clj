(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.tx :as tx]
            [crux.kv-store :as ks]
            [crux.rdf :as rdf]
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
         (#(rdf/maps-by-iri % false)))))

(t/deftest test-can-store-doc
  (let [tx-log (tx/->DocTxLog f/*kv*)
        object-store (doc/->DocObjectStore f/*kv*)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)]
    (t/is (= 47 (count picasso)))
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
        expected-entities [{:eid eid
                            :content-hash content-hash
                            :bt business-time
                            :tt transact-time
                            :tx-id tx-id}]]

    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/testing "can find entity by content hash"
        (t/is (= [eid] (doc/eids-for-content-hash snapshot content-hash))))

      (t/testing "can see entity at transact and business time"
        (t/is (= expected-entities
                 (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time)))
        (t/is (= expected-entities
                 (doc/all-entities snapshot transact-time transact-time))))

      (t/testing "can find entity by secondary index"
        (t/testing "single value attribute")
        (t/is (= expected-entities
                 (doc/entities-by-attribute-value-at snapshot :http://xmlns.com/foaf/0.1/givenName "Pablo" "Pablo" transact-time transact-time)))

        (t/testing "find multi valued attribute"
          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at
                    snapshot
                    :http://purl.org/dc/terms/subject
                    :http://dbpedia.org/resource/Category:Cubist_artists :http://dbpedia.org/resource/Category:Cubist_artists
                    transact-time transact-time))))

        (t/testing "find attribute by range"
          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at
                    snapshot
                    :http://dbpedia.org/property/imageSize
                    230 230
                    transact-time transact-time)))

          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at
                    snapshot
                    :http://dbpedia.org/property/imageSize
                    229
                    230
                    transact-time transact-time)))
          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at
                    snapshot
                    :http://dbpedia.org/property/imageSize
                    229
                    231
                    transact-time transact-time)))
          (t/is (= expected-entities
                   (doc/entities-by-attribute-value-at
                    snapshot
                    :http://dbpedia.org/property/imageSize
                    230
                    231
                    transact-time transact-time)))

          (t/is (empty?
                 (doc/entities-by-attribute-value-at
                  snapshot
                  :http://dbpedia.org/property/imageSize
                  231
                  255
                  transact-time transact-time)))
          (t/is (empty?
                 (doc/entities-by-attribute-value-at
                  snapshot
                  :http://dbpedia.org/property/imageSize
                  1
                  229
                  transact-time transact-time)))
          (t/is (empty?
                 (doc/entities-by-attribute-value-at
                  snapshot
                  :http://dbpedia.org/property/imageSize
                  -255
                  229
                  transact-time transact-time)))))

      (t/testing "cannot see entity before business or transact time"
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-20")))

        (t/is (empty? (doc/all-entities snapshot #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/all-entities snapshot transact-time #inst "2018-05-20"))))

      (t/testing "can see entity after business or transact time"
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" transact-time)))
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-22"))))

      (t/testing "can see entity history"
        (t/is (= [{:eid eid
                   :content-hash content-hash
                   :bt business-time
                   :tt transact-time
                   :tx-id tx-id}]
                 (doc/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-20"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= [eid] (doc/eids-for-content-hash snapshot new-content-hash)))
          (t/is (= [{:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= [{:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}] (doc/all-entities snapshot new-business-time new-transact-time)))

          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21"))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-22"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= [eid] (doc/eids-for-content-hash snapshot new-content-hash)))
          (t/is (= [{:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= [{:eid eid
                     :content-hash content-hash
                     :bt business-time
                     :tt transact-time
                     :tx-id tx-id}]
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time transact-time)))
          (t/is (= [{:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}] (doc/all-entities snapshot new-business-time new-transact-time))))

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

              (t/is (= [eid] (doc/eids-for-content-hash snapshot new-content-hash)))
              (t/is (= [{:eid eid
                         :content-hash new-content-hash
                         :bt new-business-time
                         :tt new-transact-time
                         :tx-id new-tx-id}]
                       (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
              (t/is (= [{:eid eid
                         :content-hash new-content-hash
                         :bt new-business-time
                         :tt new-transact-time
                         :tx-id new-tx-id}] (doc/all-entities snapshot new-business-time new-transact-time)))

              (t/is (= prev-tx-id (-> (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] prev-transact-time prev-transact-time)
                                      (first)
                                      :tx-id))))

            (t/testing "compare and set does nothing with wrong content hash"
              (let [old-picasso (assoc picasso :baz :boz)]
                @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= [{:eid eid
                             :content-hash new-content-hash
                             :bt new-business-time
                             :tt new-transact-time
                             :tx-id new-tx-id}]
                           (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time))))))

            (t/testing "compare and set updates with correct content hash"
              (let [old-picasso new-picasso
                    new-picasso (assoc old-picasso :baz :boz)
                    new-content-hash (idx/new-id new-picasso)
                    {new-transact-time :transact-time
                     new-tx-id :tx-id}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])]
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= [{:eid eid
                             :content-hash new-content-hash
                             :bt new-business-time
                             :tt new-transact-time
                             :tx-id new-tx-id}]
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

          (t/testing "eviction removes secondary indexes"
            (t/is (empty? (doc/entities-by-attribute-value-at snapshot :http://xmlns.com/foaf/0.1/givenName "Pablo" "Pablo"
                                                              new-transact-time new-transact-time)))))))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (doc/read-meta f/*kv* :foo)))
  (doc/store-meta f/*kv* :foo {:bar 2})
  (t/is (= {:bar 2} (doc/read-meta f/*kv* :foo))))
