(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.rdf :as rdf]
            [crux.fixtures :as f]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]))

(t/use-fixtures :each f/with-kv-store)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-iri % false)))))

(t/deftest test-can-store-doc
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (ByteBuffer/wrap (doc/doc->content-hash picasso))
        content-hash-hex (bu/bytes->hex (.array content-hash))]
    (t/is (= 47 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (let [ks (doc/store-docs f/*kv* [picasso])]
      (t/is (= [content-hash] ks))
      (t/is (= {content-hash
                (ByteBuffer/wrap (nippy/fast-freeze picasso))}
               (doc/docs f/*kv* ks))))

    (t/testing "non existent docs are ignored"
      (t/is (= {content-hash
                (ByteBuffer/wrap (nippy/fast-freeze picasso))}
               (doc/docs f/*kv* [content-hash-hex
                                 "090622a35d4b579d2fcfebf823821298711d3867"])))
      (t/is (empty? (doc/docs f/*kv* []))))

    (t/testing "existing doc keys"
      (t/is (= #{content-hash}
               (doc/existing-doc-keys f/*kv* [content-hash-hex
                                              "090622a35d4b579d2fcfebf823821298711d3867"]))))

    (t/testing "all existing doc keys"
      (t/is (= #{content-hash}
               (doc/all-doc-keys f/*kv*))))))

(t/deftest test-can-find-doc-by-value
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash-hex (ByteBuffer/wrap (doc/doc->content-hash picasso))]
    (doc/store-docs f/*kv* [picasso])
    (t/is (= #{content-hash-hex}
             (doc/doc-keys-by-attribute-values
              f/*kv* :http://xmlns.com/foaf/0.1/givenName #{"Pablo"})))

    (t/testing "find multi valued attribute"
      (t/is (= #{content-hash-hex}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://purl.org/dc/terms/subject #{:http://dbpedia.org/resource/Category:Cubist_artists}))))))

(t/deftest test-can-index-tx-ops
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (ByteBuffer/wrap (doc/doc->content-hash picasso))
        transact-time #inst "2018-05-21"
        tx-id 1
        eid (ByteBuffer/wrap (doc/id->bytes :http://dbpedia.org/resource/Pablo_Picasso))]

    (doc/store-docs f/*kv* [picasso])
    (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                            content-hash]] transact-time tx-id)

    (t/testing "can find entity by content hash"
      (t/is (= {content-hash [eid]}
               (doc/eids-by-content-hashes f/*kv* [content-hash]))))

    (t/testing "can see entity at transact and business time"
      (t/is (= {eid
                {:eid eid
                 :content-hash content-hash
                 :bt transact-time
                 :tt transact-time
                 :tx-id tx-id}}
               (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time)))
      (t/is (= [eid] (keys (doc/all-entities f/*kv* transact-time transact-time)))))

    (t/testing "can find entity by secondary index"
      (t/is (= [eid]
               (keys (doc/entities-by-attribute-values-at f/*kv* :http://xmlns.com/foaf/0.1/givenName #{"Pablo"} transact-time transact-time)))))

    (t/testing "cannot see entity before business or transact time"
      (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" transact-time)))
      (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-20")))

      (t/is (empty? (doc/all-entities f/*kv* #inst "2018-05-20" transact-time)))
      (t/is (empty? (doc/all-entities f/*kv* transact-time #inst "2018-05-20"))))

    (t/testing "can see entity after business or transact time"
      (t/is (some? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" transact-time)))
      (t/is (some? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-22"))))

    (t/testing "add new version of entity in the past"
      (let [new-picasso (assoc picasso :foo :bar)
            new-content-hash (ByteBuffer/wrap (doc/doc->content-hash new-picasso))
            new-transact-time #inst "2018-05-22"
            new-business-time #inst "2018-05-20"
            new-tx-id 2]
        (doc/store-docs f/*kv* [new-picasso])
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash [eid]}
                 (doc/eids-by-content-hashes f/*kv* [new-content-hash])))
        (t/is (= {eid
                  {:eid eid
                   :content-hash new-content-hash
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= [eid] (keys (doc/all-entities f/*kv* new-business-time new-transact-time))))

        (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21")))))

    (t/testing "add new version of entity in the future"
      (let [new-picasso (assoc picasso :baz :boz)
            new-content-hash (ByteBuffer/wrap (doc/doc->content-hash new-picasso))
            new-transact-time #inst "2018-05-23"
            new-business-time #inst "2018-05-22"
            new-tx-id 3]
        (doc/store-docs f/*kv* [new-picasso])
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash [eid]}
                 (doc/eids-by-content-hashes f/*kv* [new-content-hash])))
        (t/is (= {eid
                  {:eid eid
                   :content-hash new-content-hash
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= {eid
                  {:eid eid
                   :content-hash content-hash
                   :bt transact-time
                   :tt transact-time
                   :tx-id tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time #inst "2018-05-22")))
        (t/is (= [eid] (keys (doc/all-entities f/*kv* new-business-time new-transact-time))))))

    (t/testing "can correct entity at earlier business time"
      (let [new-picasso (assoc picasso :bar :foo)
            new-content-hash (ByteBuffer/wrap (doc/doc->content-hash new-picasso))
            new-transact-time #inst "2018-05-24"
            new-business-time #inst "2018-05-22"
            new-tx-id 4]
        (doc/store-docs f/*kv* [new-picasso])
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash [eid]}
                 (doc/eids-by-content-hashes f/*kv* [new-content-hash])))
        (t/is (= {eid
                  {:eid eid
                   :content-hash new-content-hash
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= [eid] (keys (doc/all-entities f/*kv* new-business-time new-transact-time))))

        (t/is (= 3 (-> (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-23" #inst "2018-05-23")
                       (get-in [eid :tx-id]))))))))
