(ns xtdb.jdbc-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.jdbc :as fj]
            [xtdb.fixtures.lubm :as fl]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [xtdb.jdbc :as j])
  (:import (java.util UUID)))

(comment
  ;; NOTE: CI does not run the tests for all dialects, to do so, run the setup code below.
  ;; pre-req: docker-compose

  (require 'clojure.java.shell)
  ;; spin up containers
  (clojure.java.shell/sh "docker-compose" "up" "-d" :dir "modules/jdbc")
  ;; set dialects to get better coverage
  (fj/set-test-dialects! :mysql :mssql :postgres :h2 :sqlite)
  ;; docker down if needed
  (clojure.java.shell/sh "docker-compose" "down" :dir "modules/jdbc")

  )
(t/use-fixtures :each fj/with-each-jdbc-dialect fj/with-jdbc-node fix/with-node fj/with-db-type)
(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:xt/id :origin-man :name "Adam"}
        submitted-tx (xt/submit-tx *api* [[::xt/put doc]])]
    (xt/await-tx *api* submitted-tx (java.time.Duration/ofSeconds 2))
    (t/is (xt/entity (xt/db *api*) :origin-man))
    (t/testing "Tx log"
      (with-open [tx-log-iterator (xt/open-tx-log *api* 0 false)]
        (t/is (= [{::xt/tx-id 2,
                   ::xt/tx-time (::xt/tx-time submitted-tx)
                   ::xt/tx-events
                   [[::xt/put
                     (c/new-id (:xt/id doc))
                     (c/hash-doc doc)]]}]
                 (iterator-seq tx-log-iterator)))))))

(t/deftest test-docs-retention
  (let [doc-store (:document-store *api*)

        doc {:xt/id :some-id, :a :b}
        doc-hash (c/hash-doc doc)

        _ (fix/submit+await-tx [[::xt/put doc]])

        docs (db/fetch-docs doc-store #{doc-hash})]

    (t/is (= 1 (count docs)))
    (t/is (= doc (-> (get docs doc-hash) (c/crux->xt))))

    (t/testing "Compaction"
      (db/submit-docs doc-store [[doc-hash :some-val]])
      (t/is (= :some-val
               (-> (db/fetch-docs doc-store #{doc-hash})
                   (get doc-hash)))))

    (t/testing "Eviction"
      (db/submit-docs doc-store [[doc-hash {:xt/id :some-id, ::xt/evicted? true}]])
      (t/is (= {:xt/id :some-id, ::xt/evicted? true}
               (-> (db/fetch-docs doc-store #{doc-hash})
                   (get doc-hash)))))

    (t/testing "Resurrect Document"
      (fix/submit+await-tx [[::xt/put doc]])

      (t/is (= doc
               (-> (db/fetch-docs doc-store #{doc-hash})
                   (get doc-hash)
                   (c/crux->xt)))))))

(t/deftest test-micro-bench
  (when (Boolean/parseBoolean (System/getenv "XTDB_JDBC_PERFORMANCE"))
    (let [n 1000
          last-tx (atom nil)]
      (time
       (dotimes [n n]
         (reset! last-tx (xt/submit-tx *api* [[::xt/put {:xt/id (keyword (str n))}]]))))

      (time
       (xt/await-tx *api* last-tx nil))))
  (t/is true))

(t/deftest test-ingest-bench
  (when (Boolean/parseBoolean (System/getenv "XTDB_JDBC_PERFORMANCE"))
    (fl/with-lubm-data
      #(t/is (= 1650
                (:num_docs (jdbc/execute-one! (:pool (:tx-log *api*))
                                              ["SELECT count(EVENT_KEY) AS num_docs FROM tx_events WHERE TOPIC = 'docs'"]
                                              {:builder-fn jdbcr/as-unqualified-lower-maps}))))))
  (t/is true))

(t/deftest test-project-star-bug-1016
  (fix/submit+await-tx [[::xt/put {:xt/id :put
                                   :xt/fn '(fn [ctx doc]
                                             [[::xt/put doc]])}]])
  (fix/submit+await-tx [[::xt/fn :put {:xt/id :foo, :foo :bar}]])

  (let [db (xt/db *api*)]

    (t/is (= #{[{:xt/id :foo, :foo :bar}]}
             (xt/q db
                   '{:find [(pull ?e [*])]
                     :where [[?e :xt/id :foo]]})))

    (t/is (= {:xt/id :foo, :foo :bar}
             (xt/entity db :foo)))

    (t/is (= #{[{:xt/id :foo, :foo :bar}]}
             (xt/q db
                   '{:find [(pull ?e [*])]
                     :where [[?e :xt/id :foo]]})))))

(t/deftest test-deadlock
  ;; SQLite doesn't support writing from multiple threads
  ;; TODO fix :h2 and :mssql - better than they were but still fail this test.

  (when-not (#{:sqlite :h2 :mssql} fj/*db-type*)
    (let [eids #{:foo :bar :baz :quux}]
      (->> (for [_ (range 100)]
             (future
               (xt/submit-tx *api* (for [eid (shuffle eids)]
                                     [::xt/put {:xt/id eid}]))))
           doall
           (run! deref))
      (t/is true))))

(t/deftest test-identity-race-condition-1603
  ;; SQLite doesn't support writing from multiple threads
  (when-not (#{:sqlite} fj/*db-type*)
    ;; order:
    ;; submit 1, submit 2, commit 1, await both, check both docs present

    ;; bug was that because tx2 committed first, the node thinks latest-completed-tx = 2,
    ;; so never plays tx1

    (let [!tx1-submitted (promise)

          !latch (promise)
          !fut1 (future
                  (let [this-thread (Thread/currentThread)
                        orig-f @#'j/insert-event!]
                    (with-redefs [j/insert-event! (fn [pool event-key v topic]
                                                    (let [res (orig-f pool event-key v topic)]
                                                      (when (and (= topic "txs")
                                                                 (= this-thread (Thread/currentThread)))
                                                        (deliver !tx1-submitted nil)
                                                        @!latch)
                                                      res))]
                      (xt/submit-tx *api* [[::xt/put {:xt/id :foo}]]))))

          _ @!tx1-submitted

          !fut2 (future
                  (fix/submit+await-tx [[::xt/put {:xt/id :bar}]]))]

      ;; gives the node enough time to spot tx2 without tx1 having committed
      (t/is (= ::timeout (deref !fut2 150 ::timeout)))

      (deliver !latch nil)

      @!fut1 @!fut2
      (xt/sync *api*)

      (let [db (xt/db *api*)]
        (t/is (= {:xt/id :foo} (xt/entity db :foo)))
        (t/is (= {:xt/id :bar} (xt/entity db :bar)))))))

(t/deftest latest-submitted-tx-1896
  (xt/submit-tx *api* [[::xt/put {:xt/id "foo"}]])
  (t/is (pos-int? (::xt/tx-id (xt/latest-submitted-tx *api*))))

  (let [offset (::xt/tx-id (xt/latest-submitted-tx *api*))
        pool (-> *api* :!system deref ::j/connection-pool :pool)
        jdbc-execute jdbc/execute!
        insert-docs! (fn [n]
                       (with-redefs [jdbc/execute! jdbc-execute]
                         (dotimes [_ n]
                           (#'j/insert-event! pool
                             (str (UUID/randomUUID))
                             (str (UUID/randomUUID))
                             "not-a-topic"))))
        captured-queries (atom [])]

    (with-redefs [jdbc/execute! (fn [db sql-vec & args] (swap! captured-queries conj sql-vec) (apply jdbc-execute db sql-vec args))]
      (insert-docs! 1)
      (t/is (= offset (::xt/tx-id (xt/latest-submitted-tx *api*))))

      (insert-docs! 512)
      (t/is (= offset (::xt/tx-id (xt/latest-submitted-tx *api*))))

      (insert-docs! 2048)
      (t/is (= offset (::xt/tx-id (xt/latest-submitted-tx *api*)))))

    ;; if you want to double-check plans, uncomment this - insert more rows if you want
    ;; to see more accurate plans
    #_
    (do
      (println fj/*db-type*)
      (t/is (pos? (count @captured-queries)))
      (doseq [[sql & params] @captured-queries]
        (try
          (prn (jdbc/execute! pool (into [(str "EXPLAIN " sql)] params)))
          (catch Throwable _ (println "EXC")))))))
