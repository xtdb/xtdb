(ns crux.doc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
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
    (doc/store f/*kv* [picasso])
    (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
             (doc/find-doc-keys-by-attribute-values
              f/*kv* :http://xmlns.com/foaf/0.1/givenName #{"Pablo"})))

    (t/testing "find multi valued attribute"
      (t/is (= #{"58232d6993e120d1aa19edfc7fbd1df791f06b48"}
               (doc/find-doc-keys-by-attribute-values
                f/*kv* :http://purl.org/dc/terms/subject #{:http://dbpedia.org/resource/Category:Cubist_artists}))))))
