(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.standalone]
            [crux.fixtures.standalone :as fs]
            [crux.codec :as c]
            [crux.fixtures.api :refer [*api*] :as fapi]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.kafka :as fk]
            crux.jdbc
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.http-server :as fh]
            [crux.rdf :as rdf]
            [crux.api :as api]
            [crux.fixtures.api :as fapi]
            [crux.db :as db]
            [crux.query :as q]
            [crux.tx :as tx]
            [crux.fixtures :as f]
            [clojure.java.io :as io])
  (:import crux.api.NodeOutOfSyncException
           java.util.Date
           java.time.Duration
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.query.Binding))

(def api-implementations
  (-> {:local-standalone (t/join-fixtures [fs/with-standalone-node kvf/with-kv-dir fapi/with-node])
       :remote (t/join-fixtures [fs/with-standalone-node kvf/with-kv-dir fh/with-http-server fapi/with-node fh/with-http-client])
       :h2 (t/join-fixtures [#(fj/with-jdbc-node :h2 %) kvf/with-kv-dir fapi/with-node])
       :sqlite (t/join-fixtures [#(fj/with-jdbc-node :sqlite %) kvf/with-kv-dir fapi/with-node])
       :local-kafka (-> (t/join-fixtures [fk/with-cluster-node-opts kvf/with-kv-dir fapi/with-node])
                        (with-meta {::embedded-kafka? true}))
       :kafka+remote-doc-store (-> (t/join-fixtures [fk/with-cluster-node-opts fs/with-standalone-doc-store kvf/with-kv-dir fapi/with-node])
                                   (with-meta {::embedded-kafka? true}))}
      (select-keys [:local-kafka])
      #_(select-keys [:local-standalone])
      #_(select-keys [:local-standalone :h2 :sqlite :remote])))

(defn- with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (t/testing (str node-type)
      (run-tests f))))

(t/use-fixtures :once
  (fn [f]
    (if (some (comp ::embedded-kafka? meta) (vals api-implementations))
      (fk/with-embedded-kafka-cluster f)
      (f))))

(t/use-fixtures :each with-each-api-implementation)

(defmacro with-both-dbs [[db db-args] & body]
  `(do
     (t/testing "with open-db"
       (with-open [~db (api/open-db ~@db-args)]
         ~@body))

     (t/testing "with db"
       (let [~db (api/db ~@db-args)]
         ~@body))))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(t/deftest test-content-hash-invalid
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}
        content-hash (str (c/new-id content-ivan))]
    (t/is (thrown-with-msg? Exception (re-pattern  (str content-hash "|HTTP status 400"))
                            (.submitTx *api* [[:crux.tx/put content-hash valid-time]])))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:crux.db/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.awaitTx *api* submitted-tx nil)
    (t/is (.entity (.db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))))

(t/deftest test-can-use-crux-ids
  (let [id #crux/id :https://adam.com
        doc {:crux.db/id id, :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.awaitTx *api* submitted-tx nil)
    (t/is (.entity (.db *api*) id))))

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}]

    (t/testing "put works with no id"
      (t/is
       (let [{:crux.tx/keys [tx-time] :as tx} (.submitTx *api* [[:crux.tx/put content-ivan valid-time]])]
         (.awaitTx *api* tx nil)
         (.db *api* valid-time tx-time))))

    (t/testing "Delete works with id"
      (t/is (.submitTx *api* [[:crux.tx/delete :ivan]])))))

(t/deftest test-can-use-api-to-access-crux
  (t/testing "status"
    (t/is (= (merge {:crux.index/index-version 6}
                    (when (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
                      {:crux.zk/zk-active? true}))
             (select-keys (.status *api*) [:crux.index/index-version :crux.zk/zk-active?]))))

  (t/testing "empty db"
    (t/is (api/db *api*)))

  (t/testing "syncing empty db"
    (t/is (nil? (api/sync *api* (Duration/ofSeconds 10)))))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:crux.tx/keys [tx-time tx-id] :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (= submitted-tx (.awaitTx *api* submitted-tx nil)))
      (t/is (true? (.hasTxCommitted *api* submitted-tx)))

      (let [status-map (.status *api*)]
        (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
        (t/is (= submitted-tx (.latestCompletedTx *api*))))

      (t/testing "query"
        (t/is (= #{[:ivan]} (.q (.db *api*)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]})))
        (t/is (= #{} (.q (.db *api* #inst "1999") '{:find  [e]
                                                    :where [[e :name "Ivan"]]})))

        (with-both-dbs [db (*api*)]
          (t/testing "query string"
            (t/is (= #{[:ivan]} (.q db "{:find [e] :where [[e :name \"Ivan\"]]}"))))

          (t/testing "query vector"
            (t/is (= #{[:ivan]} (.q db '[:find e
                                         :where [e :name "Ivan"]]))))

          (t/testing "malformed query"
            (t/is (thrown-with-msg? Exception
                                    #"(status 400|Spec assertion failed)"
                                    (.q db '{:find [e]}))))

          (t/testing "query with streaming result"
            (with-open [res (api/open-q db '{:find [e]
                                             :where [[e :name "Ivan"]]})]
              (t/is (= '([:ivan])
                       (iterator-seq res)))))

          (t/testing "query returning full results"
            (with-open [res (api/open-q db '{:find [e]
                                             :where [[e :name "Ivan"]]
                                             :full-results? true})]
              (t/is (= '([{:crux.db/id :ivan, :name "Ivan"}])
                       (iterator-seq res)))))

          (t/testing "entity"
            (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/entity db :ivan)))
            (with-both-dbs [db (*api* #inst "1999")]
              (t/is (nil? (api/entity db :ivan)))))

          (t/testing "entity-tx, document and history"
            (let [entity-tx (api/entity-tx db :ivan)
                  ivan {:crux.db/id :ivan :name "Ivan"}
                  ivan-crux-id (c/new-id ivan)]
              (t/is (= (merge submitted-tx
                              {:crux.db/id (str (c/new-id :ivan))
                               :crux.db/content-hash (str ivan-crux-id)
                               :crux.db/valid-time valid-time})
                       entity-tx))
              (t/is (= ivan (.document *api* (:crux.db/content-hash entity-tx))))
              (t/is (= {ivan-crux-id ivan} (api/documents *api* #{(:crux.db/content-hash entity-tx)})))
              (t/is (= [entity-tx] (api/history *api* :ivan)))
              (t/is (= [entity-tx] (api/history-range *api* :ivan #inst "1990" #inst "1990" tx-time tx-time)))

              (t/is (nil? (api/document *api* (c/new-id :does-not-exist))))
              (t/is (nil? (api/entity-tx (api/db *api* #inst "1999") :ivan))))))))))

(t/deftest test-sparql
  (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
    (api/await-tx *api* submitted-tx))

  (t/testing "SPARQL query"
    (when (bound? #'fh/*api-url*)
      (let [repo (SPARQLRepository. (str fh/*api-url* "/sparql"))]
        (try
          (.initialize repo)
          (with-open [conn (.getConnection repo)]
            (t/is (= #{[:ivan]} (execute-sparql conn "SELECT ?e WHERE { ?e <http://juxt.pro/crux/unqualified/name> \"Ivan\" }"))))
          (finally
            (.shutDown repo)))))))

(t/deftest test-statistics
  (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
    (api/await-tx *api* submitted-tx))

  (let [stats (api/attribute-stats *api*)]
    (t/is (= 1 (:name stats))))

  (t/testing "updated"
    (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan2"}]])]
      (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
      (t/is (true? (api/tx-committed? *api* submitted-tx))))

    (let [stats (api/attribute-stats *api*)]
      (t/is (= 2 (:name stats)))))

  (t/testing "reflect evicted documents"
    (let [now (Date.)
          submitted-tx (api/submit-tx *api* [[:crux.tx/evict :ivan]])]
      (t/is (api/await-tx *api* submitted-tx))

      ;; actual removal of the document happens asynchronously after
      ;; the transaction has been processed so waiting on the
      ;; submitted transaction time is not enough
      (while (api/entity (api/db *api*) :ivan)
        (assert (< (- (.getTime (Date.)) (.getTime now)) 4000))
        (Thread/sleep 500))

      (let [stats (api/attribute-stats *api*)]
        (t/is (= 0 (:name stats)))))))

(t/deftest test-adding-back-evicted-document
  (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo))

  (fapi/submit+await-tx [[:crux.tx/evict :foo]])
  (t/is (nil? (api/entity (api/db *api*) :foo)))

  (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo)))

(t/deftest test-document-bug-123
  (let [version-1-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1}]])]
    (.awaitTx *api* version-1-submitted-tx nil)
    (t/is (true? (.hasTxCommitted *api* version-1-submitted-tx))))

  (let [version-2-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2}]])]
    (.awaitTx *api* version-2-submitted-tx nil)
    (t/is (true? (.hasTxCommitted *api* version-2-submitted-tx))))

  (let [history (.history *api* :ivan)]
    (t/is (= 2 (count history)))
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
              {:crux.db/id :ivan :name "Ivan" :version 1}]
             (for [content-hash (map :crux.db/content-hash history)]
               (.document *api* content-hash))))))

(t/deftest test-tx-log
  (let [valid-time (Date.)
        tx1 (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]

    (t/testing "tx-log"
      (with-open [tx-log-iterator (.openTxLog *api* nil false)]
        (let [result (iterator-seq tx-log-iterator)]
          (t/is (not (realized? result)))
          (t/is (= [(assoc tx1
                      :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                   result))
          (t/is (realized? result))))

      (t/testing "with ops"
        (with-open [tx-log-iterator (.openTxLog *api* nil true)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (not (realized? result)))
            (t/is (= [(assoc tx1
                        :crux.api/tx-ops [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
                     result))
            (t/is (realized? result)))))

      (t/testing "from tx id"
        (with-open [tx-log-iterator (api/open-tx-log *api* (::tx/tx-id tx1) false)]
          (t/is (empty? (iterator-seq tx-log-iterator))))))

    (t/testing "tx log skips failed transactions"
      (let [tx2 (fapi/submit+await-tx [[:crux.tx/match :ivan {:crux.db/id :ivan :name "Ivan2"}]
                                       [:crux.tx/put {:crux.db/id :ivan :name "Ivan3"}]])]
        (t/is (false? (api/tx-committed? *api* tx2)))

        (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (= [(assoc tx1
                        :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))))

        (let [tx3 (fapi/submit+await-tx [[:crux.tx/match :ivan {:crux.db/id :ivan :name "Ivan"}]
                                         [:crux.tx/put {:crux.db/id :ivan :name "Ivan3"}]])]
          (t/is (true? (api/tx-committed? *api* tx3)))
          (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (= 2 (count result))))))))))

(t/deftest test-db-history-api
  (let [version-1-submitted-tx-time (-> (.awaitTx *api* (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1} #inst "2019-02-01"]]) nil)
                                        :crux.tx/tx-time)
        version-2-submitted-tx-time (-> (.awaitTx *api* (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2} #inst "2019-02-02"]]) nil)
                                        :crux.tx/tx-time)
        version-3-submitted-tx-time (-> (.awaitTx *api* (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 3} #inst "2019-02-03"]]) nil)
                                        :crux.tx/tx-time)
        version-2-corrected-submitted-tx-time (-> (.awaitTx *api* (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true} #inst "2019-02-02"]]) nil)
                                                  :crux.tx/tx-time)]

    (let [history (.history *api* :ivan)]
      (t/is (= 4 (count history))))

    (with-both-dbs [db (*api* #inst "2019-02-03")]
      (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}]
               (map :crux.db/doc (api/history-ascending db :ivan))))

      (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                {:crux.db/id :ivan :name "Ivan" :version 1}]
               (map :crux.db/doc (api/history-descending db :ivan))))

      (with-open [history-asc (api/open-history-ascending db :ivan)
                  history-desc (api/open-history-descending db :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}]
                 (->> (iterator-seq history-asc) (map :crux.db/doc))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (->> (iterator-seq history-desc) (map :crux.db/doc))))))

    (with-both-dbs [db (*api* #inst "2019-02-02")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc (api/history-ascending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (api/history-descending db snapshot :ivan))))))

    (with-both-dbs [db (*api* #inst "2019-01-31")]
      (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 1}
                {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                {:crux.db/id :ivan :name "Ivan" :version 3}]
               (map :crux.db/doc (api/history-ascending db :ivan))))
      (t/is (empty? (map :crux.db/doc (api/history-descending db :ivan))))

      (with-open [history-asc (api/open-history-ascending db :ivan)
                  history-desc (api/open-history-descending db :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 1}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (->> (iterator-seq history-asc) (map :crux.db/doc))))
        (t/is (empty? (iterator-seq history-desc)))))

    (with-both-dbs [db (*api* #inst "2019-02-04")]
      (with-open [history-asc (api/open-history-ascending db :ivan)
                  history-desc (api/open-history-descending db :ivan)]
        (t/is (empty? (iterator-seq history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (->> (iterator-seq history-desc) (map :crux.db/doc))))))

    (with-both-dbs [db (*api* #inst "2019-02-04" #inst "2019-01-31")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (empty? (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (with-both-dbs [db (*api* #inst "2019-02-02" version-2-submitted-tx-time)]
      (with-open [history-asc (api/open-history-ascending db :ivan)
                  history-desc (api/open-history-descending db :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}]
                 (->> (iterator-seq history-asc) (map :crux.db/doc))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (->> (iterator-seq history-desc) (map :crux.db/doc))))))

    (with-both-dbs [db (*api* #inst "2019-02-03" version-2-submitted-tx-time)]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))))

(t/deftest test-db-throws-if-future-tx-time-provided
  (let [{:keys [^Date crux.tx/tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (api/db *api* the-future the-future)))))

(t/deftest test-latest-submitted-tx
  (t/is (nil? (.latestSubmittedTx *api*)))

  (let [{:keys [crux.tx/tx-id] :as tx} (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
    (t/is (= {:crux.tx/tx-id tx-id}
             (.latestSubmittedTx *api*))))

  (api/sync *api*)

  (t/is (= {:crux.db/id :foo} (api/entity (api/db *api*) :foo))))
