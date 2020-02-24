(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.standalone]
            [crux.fixtures.standalone :as fs]
            [crux.codec :as c]
            [crux.fixtures.api :refer [*api*] :as fapi]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.kafka :as fk :refer [*ingest-client*]]
            crux.jdbc
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.http-server :as fh]
            [crux.fixtures.kafka :as kf]
            [crux.rdf :as rdf]
            [crux.api :as api]
            [crux.fixtures.api :as fapi]
            [crux.fixtures.doc-store :as ds]
            [crux.db :as db]
            [crux.query :as q]
            [crux.tx :as tx])
  (:import crux.api.NodeOutOfSyncException
           java.util.Date
           java.time.Duration
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.query.Binding))

(defn- with-each-api-implementation [f]
  (t/testing "Local API ClusterNode"
    ((t/join-fixtures [kf/with-cluster-node-opts kvf/with-kv-dir fapi/with-node]) f))
  (t/testing "Local API StandaloneNode"
    ((t/join-fixtures [fs/with-standalone-node kvf/with-kv-dir fapi/with-node]) f))
  (t/testing "JDBC Node"
    ((t/join-fixtures [#(fj/with-jdbc-node :h2 %) kvf/with-kv-dir fapi/with-node]) f))
  (t/testing "Remote API"
    ((t/join-fixtures [fs/with-standalone-node kvf/with-kv-dir fh/with-http-server
                       fapi/with-node
                       fh/with-http-client])
     f))
  (t/testing "Kafka and Remote Doc Store"
    ((t/join-fixtures [ds/with-remote-doc-store-opts kf/with-cluster-node-opts kvf/with-kv-dir fapi/with-node]) f)))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each with-each-api-implementation)

(declare execute-sparql)

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
    (t/is (= (merge {:crux.index/index-version 5}
                    (when (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
                      {:crux.zk/zk-active? true}))
             (select-keys (.status *api*) [:crux.index/index-version :crux.zk/zk-active?]))))

  (t/testing "empty db"
    (t/is (.db *api*)))

  (t/testing "syncing empty db"
    (t/is (nil? (.sync *api* (Duration/ofSeconds 10)))))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:crux.tx/keys [tx-time tx-id] :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (= submitted-tx (.awaitTx *api* submitted-tx nil)))
      (.awaitTx *api* submitted-tx nil)
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

        (t/testing "query string"
          (t/is (= #{[:ivan]} (.q (.db *api*)
                                  "{:find [e] :where [[e :name \"Ivan\"]]}"))))

        (t/testing "query vector"
          (t/is (= #{[:ivan]} (.q (.db *api*) '[:find e
                                                :where [e :name "Ivan"]]))))

        (t/testing "malformed query"
          (t/is (thrown-with-msg? Exception
                                  #"(status 400|Spec assertion failed)"
                                  (.q (.db *api*) '{:find [e]}))))

        (t/testing "query with streaming result"
          (let [db (.db *api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]})]
                (t/is (not (realized? result)))
                (t/is (= '([:ivan]) result))
                (t/is (realized? result))))))

        (t/testing "query returning full results"
          (let [db (.db *api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]
                                             :full-results? true})]
                (t/is (not (realized? result)))
                (t/is (= '([{:crux.db/id :ivan, :name "Ivan"}]) result))
                (t/is (realized? result))))))

        (t/testing "SPARQL query"
          (when (bound? #'fh/*api-url*)
            (let [repo (SPARQLRepository. (str fh/*api-url* "/sparql"))]
              (try
                (.initialize repo)
                (with-open [conn (.getConnection repo)]
                  (t/is (= #{[:ivan]} (execute-sparql conn "SELECT ?e WHERE { ?e <http://juxt.pro/crux/unqualified/name> \"Ivan\" }"))))
                (finally
                  (.shutDown repo)))))))

      (t/testing "entity"
        (t/is (= {:crux.db/id :ivan :name "Ivan"} (.entity (.db *api*) :ivan)))
        (t/is (nil? (.entity (.db *api* #inst "1999") :ivan))))

      (t/testing "entity-tx, document and history"
        (let [entity-tx (.entityTx (.db *api*) :ivan)
              ivan {:crux.db/id :ivan :name "Ivan"}
              ivan-crux-id (c/new-id ivan)]
          (t/is (= (merge submitted-tx
                          {:crux.db/id           (str (c/new-id :ivan))
                           :crux.db/content-hash (str ivan-crux-id)
                           :crux.db/valid-time   valid-time})
                   entity-tx))
          (t/is (= ivan (.document *api* (:crux.db/content-hash entity-tx))))
          (t/is (= {ivan-crux-id ivan} (.documents *api* #{(:crux.db/content-hash entity-tx)})))
          (t/is (= [entity-tx] (.history *api* :ivan)))
          (t/is (= [entity-tx] (.historyRange *api* :ivan #inst "1990" #inst "1990" tx-time tx-time)))

          (t/is (nil? (.document *api* (c/new-id :does-not-exist))))
          (t/is (nil? (.entityTx (.db *api* #inst "1999") :ivan)))))

      (t/testing "statistics"
        (let [stats (.attributeStats *api*)]
          (t/is (= 1 (:name stats))))

        (t/testing "updated"
          (let [valid-time (Date.)
                submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan2"} valid-time]])]
            (.awaitTx *api* submitted-tx nil)
            (t/is (true? (.hasTxCommitted *api* submitted-tx)))
            (t/is (= submitted-tx (.awaitTx *api* submitted-tx nil))))

          (let [stats (.attributeStats *api*)]
            (t/is (= 2 (:name stats)))))

        (t/testing "reflect evicted documents"
          (let [valid-time (Date.)
                submitted-tx (.submitTx *api* [[:crux.tx/evict :ivan]])]
            (t/is (.awaitTx *api* submitted-tx nil))

            ;; actual removal of the document happens asynchronously after
            ;; the transaction has been processed so waiting on the
            ;; submitted transaction time is not enough
            (while (.entity (.db *api*) :ivan)
              (assert (< (- (.getTime (Date.)) (.getTime valid-time)) 4000))
              (Thread/sleep 500))

            (let [stats (.attributeStats *api*)]
              (t/is (= 0 (:name stats))))))))))

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
        (with-open [tx-log-iterator (api/open-tx-log *api* (inc (::tx/tx-id tx1)) false)]
          (t/is (empty? (iterator-seq tx-log-iterator))))))

    (t/testing "tx log skips failed transactions"
      (let [tx2 (fapi/submit+await-tx [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan2"} {:crux.db/id :ivan :name "Ivan3"}]])]
        (t/is (false? (api/tx-committed? *api* tx2)))

        (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (= [(assoc tx1
                        :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))))

        (let [tx3 (fapi/submit+await-tx [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan"} {:crux.db/id :ivan :name "Ivan3"}]])]
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

    (let [db (.db *api* #inst "2019-02-03")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-02-02")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-01-31")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 1}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (empty? (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-02-04")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-02-04" #inst "2019-01-31")]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (empty? (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-02-02" version-2-submitted-tx-time)]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}]
                 (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

    (let [db (.db *api* #inst "2019-02-03" version-2-submitted-tx-time)]
      (with-open [snapshot (.newSnapshot db)]
        (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc (.historyDescending db snapshot :ivan))))))))

(t/deftest test-ingest-client
  (if (and (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
           (instance? crux.kafka.KafkaDocumentStore (:document-store *api*)))
    (kf/with-ingest-client
      (fn []
        (let [submitted-tx @(.submitTxAsync *ingest-client* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
          (.awaitTx *api* submitted-tx nil)
          (t/is (true? (.hasTxCommitted *api* submitted-tx)))
          (t/is (= #{[:ivan]} (.q (.db *api*)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]})))

          (with-open [tx-log-iterator (.openTxLog *ingest-client* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                               :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"})]])]
                       result))
              (t/is (realized? result))))

          (t/is (thrown? IllegalArgumentException (.openTxLog *ingest-client* nil true))))))
    (t/is true)))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

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
