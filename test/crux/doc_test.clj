(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.doc.index :as idx]
            [crux.doc.tx :as tx]
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
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)
        content-hash-hex (str content-hash)]
    (t/is (= 47 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (doc/store-doc f/*kv* content-hash picasso)
    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/is (= {content-hash
                (ByteBuffer/wrap (nippy/fast-freeze picasso))}
               (doc/docs snapshot [content-hash]))))

    (t/testing "non existent docs are ignored"
      (with-open [snapshot (ks/new-snapshot f/*kv*)]
        (t/is (= {content-hash
                  (ByteBuffer/wrap (nippy/fast-freeze picasso))}
                 (doc/docs snapshot [content-hash-hex
                                     "090622a35d4b579d2fcfebf823821298711d3867"])))
        (t/is (empty? (doc/docs snapshot [])))))

    (t/testing "all existing doc keys"
      (t/is (= #{content-hash}
               (with-open [snapshot (ks/new-snapshot f/*kv*)]
                 (doc/all-doc-keys snapshot)))))))

(t/deftest test-can-find-doc-by-value
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)]
    (doc/store-doc f/*kv* content-hash picasso)
    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                snapshot :http://xmlns.com/foaf/0.1/givenName #{"Pablo"}))))

    (t/testing "find multi valued attribute"
      (with-open [snapshot (ks/new-snapshot f/*kv*)]
        (t/is (= #{content-hash}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://purl.org/dc/terms/subject #{:http://dbpedia.org/resource/Category:Cubist_artists})))))

    (t/testing "find attribute by range"
      (with-open [snapshot (ks/new-snapshot f/*kv*)]
        (t/is (= #{content-hash}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize #{230})))

        (t/is (= #{content-hash}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[229 230]])))
        (t/is (= #{content-hash}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[229 231]])))
        (t/is (= #{content-hash}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[230 231]])))

        (t/is (= #{}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[231 255]])))
        (t/is (= #{}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[1 229]])))
        (t/is (= #{}
                 (doc/doc-keys-by-attribute-values
                  snapshot :http://dbpedia.org/property/imageSize [[-255 229]])))))))

(t/deftest test-can-index-tx-ops
  (let [tx-log (tx/->DocTxLog f/*kv*)
        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (idx/new-id picasso)
        business-time #inst "2018-05-21"
        eid (idx/new-id :http://dbpedia.org/resource/Pablo_Picasso)
        {:keys [transact-time tx-id]}
        @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso picasso business-time]])]

    (with-open [snapshot (ks/new-snapshot f/*kv*)]
      (t/testing "can find entity by content hash"
        (t/is (= {content-hash [eid]}
                 (doc/eids-by-content-hashes snapshot [content-hash]))))

      (t/testing "can see entity at transact and business time"
        (t/is (= {eid
                  {:eid eid
                   :content-hash content-hash
                   :bt business-time
                   :tt transact-time
                   :tx-id tx-id}}
                 (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time)))
        (t/is (= [eid] (keys (doc/all-entities snapshot transact-time transact-time)))))

      (t/testing "can find entity by secondary index"
        (t/is (= [eid]
                 (keys (doc/entities-by-attribute-values-at snapshot :http://xmlns.com/foaf/0.1/givenName #{"Pablo"} transact-time transact-time)))))

      (t/testing "cannot see entity before business or transact time"
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-20")))

        (t/is (empty? (doc/all-entities snapshot #inst "2018-05-20" transact-time)))
        (t/is (empty? (doc/all-entities snapshot transact-time #inst "2018-05-20"))))

      (t/testing "can see entity after business or transact time"
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" transact-time)))
        (t/is (some? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-22")))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-20"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= {new-content-hash [eid]}
                   (doc/eids-by-content-hashes snapshot [new-content-hash])))
          (t/is (= {eid
                    {:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}}
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= [eid] (keys (doc/all-entities snapshot new-business-time new-transact-time))))

          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21"))))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (idx/new-id new-picasso)
            new-business-time #inst "2018-05-22"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (= {new-content-hash [eid]}
                   (doc/eids-by-content-hashes snapshot [new-content-hash])))
          (t/is (= {eid
                    {:eid eid
                     :content-hash new-content-hash
                     :bt new-business-time
                     :tt new-transact-time
                     :tx-id new-tx-id}}
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
          (t/is (= {eid
                    {:eid eid
                     :content-hash content-hash
                     :bt business-time
                     :tt transact-time
                     :tx-id tx-id}}
                   (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time transact-time)))
          (t/is (= [eid] (keys (doc/all-entities snapshot new-business-time new-transact-time)))))

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

              (t/is (= {new-content-hash [eid]}
                       (doc/eids-by-content-hashes snapshot [new-content-hash])))
              (t/is (= {eid
                        {:eid eid
                         :content-hash new-content-hash
                         :bt new-business-time
                         :tt new-transact-time
                         :tx-id new-tx-id}}
                       (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
              (t/is (= [eid] (keys (doc/all-entities snapshot new-business-time new-transact-time))))

              (t/is (= prev-tx-id (-> (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] prev-transact-time prev-transact-time)
                                      (get-in [eid :tx-id])))))

            (t/testing "compare and set does nothing with wrong content hash"
              (let [old-picasso (assoc picasso :baz :boz)]
                @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= {eid
                            {:eid eid
                             :content-hash new-content-hash
                             :bt new-business-time
                             :tt new-transact-time
                             :tx-id new-tx-id}}
                           (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time))))))

            (t/testing "compare and set updates with correct content hash"
              (let [old-picasso new-picasso
                    new-picasso (assoc old-picasso :baz :boz)
                    new-content-hash (idx/new-id new-picasso)
                    {new-transact-time :transact-time
                     new-tx-id :tx-id}
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-business-time]])]
                (with-open [snapshot (ks/new-snapshot f/*kv*)]
                  (t/is (= {eid
                            {:eid eid
                             :content-hash new-content-hash
                             :bt new-business-time
                             :tt new-transact-time
                             :tx-id new-tx-id}}
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
                                   (get-in [eid :tx-id]))))))))))

    (t/testing "can retrieve history of entity"
      (with-open [snapshot (ks/new-snapshot f/*kv*)]
        (let [picasso-history (get (doc/entity-histories snapshot [:http://dbpedia.org/resource/Pablo_Picasso]) eid)]
          (t/is (= 6 (count (map :content-hash picasso-history))))
          (t/is (= 5 (count (doc/docs snapshot (keep :content-hash picasso-history))))))))

    (t/testing "can evict entity"
      (let [new-business-time #inst "2018-05-23"
            {new-transact-time :transact-time
             new-tx-id :tx-id}
            @(db/submit-tx tx-log [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso new-business-time]])]

        (with-open [snapshot (ks/new-snapshot f/*kv*)]
          (t/is (empty? (doc/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))

          (t/testing "eviction adds to and keeps tx history"
            (let [picasso-history (get (doc/entity-histories snapshot [:http://dbpedia.org/resource/Pablo_Picasso]) eid)]
              (t/is (= 7 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (doc/docs snapshot (keep :content-hash picasso-history)))))))

          (t/testing "eviction removes secondary indexes"
            (t/is (empty? (keys (doc/entities-by-attribute-values-at snapshot :http://xmlns.com/foaf/0.1/givenName #{"Pablo"} new-transact-time new-transact-time))))
            (t/is (empty? (doc/doc-keys-by-attribute-values snapshot :http://xmlns.com/foaf/0.1/givenName #{"Pablo"})))))))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (doc/read-meta f/*kv* :foo)))
  (doc/store-meta f/*kv* :foo {:bar 2})
  (t/is (= {:bar 2} (doc/read-meta f/*kv* :foo))))
