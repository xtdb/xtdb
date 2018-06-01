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
        content-hash (doc/doc->content-hash picasso)
        content-hash-hex (str content-hash)]
    (t/is (= 47 (count picasso)))
    (t/is (= "Pablo" (:http://xmlns.com/foaf/0.1/givenName picasso)))

    (let [ks (doc/store-docs f/*kv* {content-hash picasso})]
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

    (t/testing "all existing doc keys"
      (t/is (= #{content-hash}
               (doc/all-doc-keys f/*kv*))))))

(t/deftest test-can-find-doc-by-value
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (doc/doc->content-hash picasso)]
    (doc/store-docs f/*kv* {content-hash picasso})
    (t/is (= #{content-hash}
             (doc/doc-keys-by-attribute-values
              f/*kv* :http://xmlns.com/foaf/0.1/givenName #{"Pablo"})))

    (t/testing "find multi valued attribute"
      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://purl.org/dc/terms/subject #{:http://dbpedia.org/resource/Category:Cubist_artists}))))

    (t/testing "find attribute by range"
      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize #{230})))

      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[229 230]])))
      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[229 231]])))
      (t/is (= #{content-hash}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[230 231]])))

      (t/is (= #{}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[231 255]])))
      (t/is (= #{}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[1 229]])))
      (t/is (= #{}
               (doc/doc-keys-by-attribute-values
                f/*kv* :http://dbpedia.org/property/imageSize [[-255 229]]))))))

(defn local-transact [kv eid doc business-time transact-time tx-id]
  (let [content-hash (doc/doc->content-hash doc)]
    (doc/store-docs kv {content-hash doc})
    (doc/store-txs kv nil [[:crux.tx/put eid content-hash business-time]] transact-time tx-id)))

(t/deftest test-can-index-tx-ops
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (doc/doc->content-hash picasso)
        transact-time #inst "2018-05-21"
        tx-id 1
        eid (doc/->Id (doc/id->bytes :http://dbpedia.org/resource/Pablo_Picasso))]

    (local-transact f/*kv* :http://dbpedia.org/resource/Pablo_Picasso picasso transact-time transact-time tx-id)

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
            new-content-hash (doc/doc->content-hash new-picasso)
            new-transact-time #inst "2018-05-22"
            new-business-time #inst "2018-05-20"
            new-tx-id 2]
        (local-transact f/*kv* :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time new-transact-time new-tx-id)

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
            new-content-hash (doc/doc->content-hash new-picasso)
            new-transact-time #inst "2018-05-23"
            new-business-time #inst "2018-05-22"
            new-tx-id 3]
        (local-transact f/*kv* :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time new-transact-time new-tx-id)

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
            new-content-hash (doc/doc->content-hash new-picasso)
            new-transact-time #inst "2018-05-24"
            new-business-time #inst "2018-05-22"
            new-tx-id 4]
        (local-transact f/*kv* :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-business-time new-transact-time new-tx-id)

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
                       (get-in [eid :tx-id]))))))

    (t/testing "can retrieve history of entity"
      (let [picasso-history (get (doc/entity-histories f/*kv* [:http://dbpedia.org/resource/Pablo_Picasso]) eid)]
        (t/is (= [4 3 1 2] (map :tx-id picasso-history)))
        (t/is (= 4 (count (set (map :content-hash picasso-history)))))))))

(t/deftest test-store-and-retrieve-meta
  (t/is (nil? (doc/read-meta f/*kv* :foo)))
  (doc/store-meta f/*kv* :foo {:bar 2})
  (t/is (= {:bar 2} (doc/read-meta f/*kv* :foo))))
