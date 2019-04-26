(ns crux.tx-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [crux.byte-utils :as bu]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures :as f]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [taoensso.nippy :as nippy]
            [crux.bootstrap :as b])
  (:import java.util.Date))


(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store f/with-silent-test-check)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % :en))
         (#(rdf/maps-by-id %)))))

;; TODO: This is a large, useful, test that exercises many parts, but
;; might be better split up.
(t/deftest test-can-index-tx-ops-acceptance-test
  (let [object-store (f/kv-object-store-w-cache f/*kv*)

        tx-log (f/kv-tx-log f/*kv* object-store)

        indexer (tx/->KvIndexer f/*kv* tx-log object-store)

        picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)
        content-hash (c/new-id picasso)
        valid-time #inst "2018-05-21"
        eid (c/new-id :http://dbpedia.org/resource/Pablo_Picasso)
        {:crux.tx/keys [tx-time tx-id]}
        @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso picasso valid-time]])
        expected-entities [(c/map->EntityTx {:eid          eid
                                             :content-hash content-hash
                                             :vt           valid-time
                                             :tt           tx-time
                                             :tx-id        tx-id})]]

    (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-valid-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
            @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-valid-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                @(db/submit-tx tx-log [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso new-picasso new-valid-time]])]

            (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-valid-time]])]
                (t/is (= cas-failure-tx-time (tx/await-tx-time indexer cas-failure-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso old-picasso new-picasso new-valid-time]])]
                (t/is (= new-tx-time (tx/await-tx-time indexer new-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                    @(db/submit-tx tx-log [[:crux.tx/cas :http://dbpedia.org/resource/Pablo2 nil new-picasso new-valid-time]])]
                (t/is (= new-tx-time (tx/await-tx-time indexer new-tx-time {:crux.tx-log/await-tx-timeout 1000})))
                (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
                @(db/submit-tx tx-log [[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso new-valid-time]])]
            (with-open [snapshot (kv/new-snapshot f/*kv*)]
              (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))
              (t/testing "first version of entity is still visible in the past"
                (t/is (= tx-id (-> (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] valid-time new-tx-time)
                                   (first)
                                   :tx-id)))))))))

    (t/testing "can retrieve history of entity"
      (with-open [snapshot (kv/new-snapshot f/*kv*)]
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
            _ (with-open [snapshot (kv/new-snapshot f/*kv*)]
                (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
                  (db/get-objects object-store snapshot (keep :content-hash picasso-history))))

            {new-tx-time :crux.tx/tx-time
             new-tx-id   :crux.tx/tx-id}
            @(db/submit-tx tx-log [[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso #inst "1970" new-valid-time]])]

        (with-open [snapshot (kv/new-snapshot f/*kv*)]
          (t/is (empty? (idx/entities-at snapshot [:http://dbpedia.org/resource/Pablo_Picasso] new-valid-time new-tx-time)))

          (t/testing "eviction adds to and keeps tx history"
            (let [picasso-history (idx/entity-history snapshot :http://dbpedia.org/resource/Pablo_Picasso)]
              (t/is (= 7 (count (map :content-hash picasso-history))))
              (t/testing "eviction removes docs"
                (t/is (empty? (db/get-objects object-store snapshot (keep :content-hash picasso-history)))))
              (t/testing "eviction removes secondary indexes"
                (with-open [i (kv/new-iterator snapshot)]
                  (doseq [{:keys [content-hash]} picasso-history
                          :let [version-k (c/encode-attribute+entity+content-hash+value-key-to
                                            nil
                                            (c/->id-buffer :http://xmlns.com/foaf/0.1/givenName)
                                            (c/->id-buffer :http://dbpedia.org/resource/Pablo_Picasso)
                                            (c/->id-buffer content-hash)
                                            (c/->value-buffer "Pablo"))]]
                    (t/is (nil? (kv/get-value snapshot version-k)))))))))))))


