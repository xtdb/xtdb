(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.rdf :as rdf]
            [crux.fixtures :as f]))

(t/use-fixtures :each f/with-kv-store)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-iri % false)))))

(t/deftest test-can-store-doc
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)]
    (t/is (= 47 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (let [ks (doc/store-docs f/*kv* [picasso])]
      (t/is (= ["58232d6993e120d1aa19edfc7fbd1df791f06b48"] ks))
      (t/is (= {"58232d6993e120d1aa19edfc7fbd1df791f06b48"
                picasso}
               (doc/docs f/*kv* ks))))

    (t/testing "non existent docs are ignored"
      (t/is (= {"58232d6993e120d1aa19edfc7fbd1df791f06b48"
                picasso}
               (doc/docs f/*kv* ["58232d6993e120d1aa19edfc7fbd1df791f06b48"
                                 "090622a35d4b579d2fcfebf823821298711d3867"])))
      (t/is (empty? (doc/docs f/*kv* []))))

    (t/testing "existing doc keys"
      (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
               (doc/existing-doc-keys f/*kv* ["58232d6993e120d1aa19edfc7fbd1df791f06b48"
                                              "090622a35d4b579d2fcfebf823821298711d3867"]))))

    (t/testing "all existing doc keys"
      (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
               (doc/all-doc-keys f/*kv*))))))

(t/deftest test-can-find-doc-by-value
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)]
    (doc/store-docs f/*kv* [picasso])
    (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
             (doc/doc-keys-by-attribute-values
              f/*kv* :http://xmlns.com/foaf/0.1/givenName #{"Pablo"})))

    (t/testing "find multi valued attribute"
      (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://purl.org/dc/terms/subject #{:http://dbpedia.org/resource/Category:Cubist_artists}))))))

(t/deftest test-can-index-tx-ops
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash-hex (bu/bytes->hex (doc/doc->content-hash picasso))
        transact-time #inst "2018-05-21"
        tx-id 0
        entity (bu/bytes->hex (doc/entity->eid-bytes :http://dbpedia.org/resource/Pablo_Picasso))]

    (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                            content-hash-hex]] transact-time tx-id)

    (t/testing "can find entity by content hash"
      (t/is (= {content-hash-hex [entity]}
               (doc/entities-by-content-hashes f/*kv* [content-hash-hex]))))

    (t/testing "can see entity at transact and business time"
      (t/is (= {entity
                {:entity entity
                 :content-hash content-hash-hex
                 :bt transact-time
                 :tt transact-time
                 :tx-id tx-id}}
               (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time transact-time)))
      (t/is (= #{entity} (doc/all-entities f/*kv* transact-time transact-time))))

    (t/testing "cannot see entity before business or transact time"
      (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" transact-time)))
      (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-20")))

      (t/is (empty? (doc/all-entities f/*kv* #inst "2018-05-20" transact-time)))
      (t/is (empty? (doc/all-entities f/*kv* transact-time #inst "2018-05-20"))))

    (t/testing "can see entity after business or transact time"
      (t/is (some? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-22" transact-time)))
      (t/is (some? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] transact-time #inst "2018-05-22"))))

    (t/testing "add new version of entity in the past"
      (let [new-content-hash-hex (bu/bytes->hex (doc/doc->content-hash (assoc picasso :foo :bar)))
            new-transact-time #inst "2018-05-22"
            new-business-time #inst "2018-05-20"
            new-tx-id 1]
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash-hex new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash-hex [entity]}
                 (doc/entities-by-content-hashes f/*kv* [new-content-hash-hex])))
        (t/is (= {entity
                  {:entity entity
                   :content-hash new-content-hash-hex
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= #{entity} (doc/all-entities f/*kv* new-business-time new-transact-time)))

        (t/is (empty? (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-20" #inst "2018-05-21")))))

    (t/testing "add new version of entity in the future"
      (let [new-content-hash-hex (bu/bytes->hex (doc/doc->content-hash (assoc picasso :baz :boz)))
            new-transact-time #inst "2018-05-23"
            new-business-time #inst "2018-05-22"
            new-tx-id 2]
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash-hex new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash-hex [entity]}
                 (doc/entities-by-content-hashes f/*kv* [new-content-hash-hex])))
        (t/is (= {entity
                  {:entity entity
                   :content-hash new-content-hash-hex
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= {entity
                  {:entity entity
                   :content-hash content-hash-hex
                   :bt transact-time
                   :tt transact-time
                   :tx-id tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time #inst "2018-05-22")))
        (t/is (= #{entity} (doc/all-entities f/*kv* new-business-time new-transact-time)))))

    (t/testing "can correct entity at earlier business time"
      (let [new-content-hash-hex (bu/bytes->hex (doc/doc->content-hash (assoc picasso :bar :foo)))
            new-transact-time #inst "2018-05-24"
            new-business-time #inst "2018-05-22"
            new-tx-id 3]
        (doc/store-txs f/*kv* [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
                                new-content-hash-hex new-business-time]] new-transact-time new-tx-id)
        (t/is (= {new-content-hash-hex [entity]}
                 (doc/entities-by-content-hashes f/*kv* [new-content-hash-hex])))
        (t/is (= {entity
                  {:entity entity
                   :content-hash new-content-hash-hex
                   :bt new-business-time
                   :tt new-transact-time
                   :tx-id new-tx-id}}
                 (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] new-business-time new-transact-time)))
        (t/is (= #{entity} (doc/all-entities f/*kv* new-business-time new-transact-time)))

        (t/is (= 2 (-> (doc/entities-at f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso] #inst "2018-05-23" #inst "2018-05-23")
                       (get-in [entity :tx-id]))))))))
