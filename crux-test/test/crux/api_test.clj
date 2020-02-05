(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.standalone]
            [crux.fixtures.standalone :as fs]
            [crux.moberg]
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
            [crux.query :as q])
  (:import crux.api.NodeOutOfSyncException
           java.util.Date
           java.time.Duration
           crux.moberg.MobergTxLog
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
                            (api/submit-tx *api* [[:crux.tx/put content-hash valid-time]])))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:crux.db/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:crux.tx/put doc]])]
    (api/await-tx *api* submitted-tx nil)
    (t/is (api/entity (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))))

(t/deftest test-can-use-crux-ids
  (let [id #crux/id :https://adam.com
        doc {:crux.db/id id, :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:crux.tx/put doc]])]
    (api/await-tx *api* submitted-tx nil)
    (t/is (api/entity (api/db *api*) id))))

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}]

    (t/testing "put works with no id"
      (t/is
       (let [{:crux.tx/keys [tx-time] :as tx} (api/submit-tx *api* [[:crux.tx/put content-ivan valid-time]])]
         (api/await-tx *api* tx nil)
         (api/db *api* valid-time tx-time))))

    (t/testing "Delete works with id"
      (t/is (api/submit-tx *api* [[:crux.tx/delete :ivan]])))))

(t/deftest test-can-use-api-to-access-crux
  (t/testing "status"
    (t/is (= (merge {:crux.index/index-version 5}
                    (when (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
                      {:crux.zk/zk-active? true}))
             (select-keys (api/status *api*) [:crux.index/index-version :crux.zk/zk-active?]))))

  (t/testing "empty db"
    (t/is (api/db *api*)))

  (t/testing "syncing empty db"
    (t/is (nil? (api/sync *api* (Duration/ofSeconds 10)))))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:crux.tx/keys [tx-time tx-id] :as submitted-tx} (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (= submitted-tx (api/await-tx *api* submitted-tx nil)))
      (api/await-tx *api* submitted-tx nil)
      (t/is (true? (api/tx-committed? *api* submitted-tx)))

      (let [status-map (api/status *api*)]
        (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
        (t/is (= submitted-tx (:crux.tx/latest-completed-tx status-map))))

      (t/testing "query"
        (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                   '{:find [e]
                                     :where [[e :name "Ivan"]]})))
        (t/is (= #{} (api/q (api/db *api* #inst "1999")
                            '{:find [e]
                              :where [[e :name "Ivan"]]})))

        (t/testing "query string"
          (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                     "{:find [e] :where [[e :name \"Ivan\"]]}"))))

        (t/testing "query vector"
          (t/is (= #{[:ivan]} (api/q (api/db *api*) '[:find e
                                                      :where [e :name "Ivan"]]))))

        (t/testing "malformed query"
          (t/is (thrown-with-msg? Exception
                                  #"(status 400|Spec assertion failed)"
                                  (api/q (api/db *api*) '{:find [e]}))))

        (t/testing "query with streaming result"
          (let [db (api/db *api*)]
            (with-open [result (api/open-q db '{:find [e]
                                                :where [[e :name "Ivan"]]})]
              (t/is (not (realized? result)))
              (t/is (= '([:ivan]) result))
              (t/is (realized? result)))))

        (t/testing "query returning full results"
          (let [db (api/db *api*)]
            (with-open [result (api/open-q db '{:find [e]
                                                :where [[e :name "Ivan"]]
                                                :full-results? true})]
              (t/is (not (realized? result)))
              (t/is (= '([{:crux.db/id :ivan, :name "Ivan"}]) result))
              (t/is (realized? result)))))

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
        (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/entity (api/db *api*) :ivan)))
        (t/is (nil? (api/entity (api/db *api* #inst "1999") :ivan))))

      (t/testing "entity-tx, document and history"
        (let [entity-tx (api/entity-tx (api/db *api*) :ivan)
              ivan {:crux.db/id :ivan :name "Ivan"}
              ivan-crux-id (c/new-id ivan)]
          (t/is (= (merge submitted-tx
                          {:crux.db/id           (str (c/new-id :ivan))
                           :crux.db/content-hash (str ivan-crux-id)
                           :crux.db/valid-time   valid-time})
                   entity-tx))
          (t/is (= ivan (api/document *api* (:crux.db/content-hash entity-tx))))
          (t/is (= {ivan-crux-id ivan} (api/documents *api* #{(:crux.db/content-hash entity-tx)})))
          (t/is (= [entity-tx] (api/history *api* :ivan)))
          (t/is (= [entity-tx] (api/history-range *api* :ivan #inst "1990" #inst "1990" tx-time tx-time)))

          (t/is (nil? (api/document *api* (c/new-id :does-not-exist))))
          (t/is (nil? (api/entity-tx (api/db *api* #inst "1999") :ivan)))))

      (t/testing "tx-log"
        (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (not (realized? result)))
            (t/is (= [(assoc submitted-tx
                        :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))
            (t/is (realized? result))))

        (t/testing "with ops"
          (with-open [tx-log-iterator (api/open-tx-log *api* nil true)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                          :crux.api/tx-ops [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
                       result))
              (t/is (realized? result)))))

        (t/testing "from tx id"
          (with-open [tx-log-iterator (api/open-tx-log *api* (inc tx-id) false)]
            (t/is (empty? (iterator-seq tx-log-iterator))))))

      (t/testing "statistics"
        (let [stats (api/attribute-stats *api*)]
          (t/is (= 1 (:name stats))))

        (t/testing "updated"
          (let [valid-time (Date.)
                submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan2"} valid-time]])]
            (api/await-tx *api* submitted-tx nil)
            (t/is (true? (api/tx-committed? *api* submitted-tx)))
            (t/is (= submitted-tx (api/await-tx *api* submitted-tx nil))))

          (let [stats (api/attribute-stats *api*)]
            (t/is (= 2 (:name stats)))))

        (t/testing "reflect evicted documents"
          (let [valid-time (Date.)
                submitted-tx (api/submit-tx *api* [[:crux.tx/evict :ivan]])]
            (t/is (api/await-tx *api* submitted-tx nil))

            ;; actual removal of the document happens asynchronously after
            ;; the transaction has been processed so waiting on the
            ;; submitted transaction time is not enough
            (while (api/entity (api/db *api*) :ivan)
              (assert (< (- (.getTime (Date.)) (.getTime valid-time)) 4000))
              (Thread/sleep 500))

            (let [stats (api/attribute-stats *api*)]
              (t/is (= 0 (:name stats))))))

        (t/testing "Add back evicted document"
          (assert (not (api/entity (api/db *api*) :ivan)))
          (let [valid-time (Date.)
                submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
            (t/is (api/await-tx *api* submitted-tx nil))
            (t/is (api/entity (api/db *api*) :ivan))))))))

(t/deftest test-document-bug-123
  (let [version-1-submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1}]])]
    (api/await-tx *api* version-1-submitted-tx nil)
    (t/is (true? (api/tx-committed? *api* version-1-submitted-tx))))

  (let [version-2-submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2}]])]
    (api/await-tx *api* version-2-submitted-tx nil)
    (t/is (true? (api/tx-committed? *api* version-2-submitted-tx))))

  (let [history (api/history *api* :ivan)]
    (t/is (= 2 (count history)))
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
              {:crux.db/id :ivan :name "Ivan" :version 1}]
             (for [content-hash (map :crux.db/content-hash history)]
               (api/document *api* content-hash))))))

(t/deftest test-tx-log-skips-failed-transactions
  (let [valid-time (Date.)
        submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
    (api/await-tx *api* submitted-tx nil)
    (t/is (true? (api/tx-committed? *api* submitted-tx)))
    (let [version-2-submitted-tx (api/submit-tx *api* [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan2"} {:crux.db/id :ivan :name "Ivan3"}]])]
      (api/await-tx *api* version-2-submitted-tx nil)
      (t/is (false? (api/tx-committed? *api* version-2-submitted-tx)))
      (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
        (let [result (iterator-seq tx-log-iterator)]
          (t/is (= [(assoc submitted-tx
                      :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                   result))))

      (let [version-3-submitted-tx (api/submit-tx *api* [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan"} {:crux.db/id :ivan :name "Ivan3"}]])]
        (api/await-tx *api* version-3-submitted-tx nil)
        (t/is (true? (api/tx-committed? *api* version-3-submitted-tx)))
        (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (= 2 (count result)))))))))

(t/deftest test-db-history-api
  (let [version-1-submitted-tx-time (-> (api/await-tx *api* (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1} #inst "2019-02-01"]]) nil)
                                        :crux.tx/tx-time)
        version-2-submitted-tx-time (-> (api/await-tx *api* (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2} #inst "2019-02-02"]]) nil)
                                        :crux.tx/tx-time)
        version-3-submitted-tx-time (-> (api/await-tx *api* (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 3} #inst "2019-02-03"]]) nil)
                                        :crux.tx/tx-time)
        version-2-corrected-submitted-tx-time (-> (api/await-tx *api* (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true} #inst "2019-02-02"]]) nil)
                                                  :crux.tx/tx-time)]

    (let [history (api/history *api* :ivan)]
      (t/is (= 4 (count history))))

    (let [db (api/db *api* #inst "2019-02-03")]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-02-02")]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-01-31")]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 1}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 3}]
                 (map :crux.db/doc history-asc)))
        (t/is (empty? (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-02-04")]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (empty? (map :crux.db/doc history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                  {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-02-04" #inst "2019-01-31")]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (empty? (map :crux.db/doc history-asc)))
        (t/is (empty? (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-02-02" version-2-submitted-tx-time)]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}]
                 (map :crux.db/doc history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc history-desc)))))

    (let [db (api/db *api* #inst "2019-02-03" version-2-submitted-tx-time)]
      (with-open [read-tx (api/open-read-tx db)
                  history-asc (api/open-history-ascending read-tx :ivan)
                  history-desc (api/open-history-descending read-tx :ivan)]
        (t/is (empty? (map :crux.db/doc history-asc)))
        (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                  {:crux.db/id :ivan :name "Ivan" :version 1}]
                 (map :crux.db/doc history-desc)))))))

(t/deftest test-ingest-client
  (if (and (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
           (instance? crux.kafka.KafkaDocumentStore (:document-store *api*)))
    (kf/with-ingest-client
      (fn []
        (let [submitted-tx @(api/submit-tx-async *ingest-client* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
          (api/await-tx *api* submitted-tx nil)
          (t/is (true? (api/tx-committed? *api* submitted-tx)))
          (t/is (= #{[:ivan]} (api/q (api/db *api*)

                                     '{:find [e]
                                       :where [[e :name "Ivan"]]})))

          (with-open [tx-log-iterator (api/open-tx-log *ingest-client* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                          :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"})]])]
                       result))
              (t/is (realized? result))))

          (t/is (thrown? IllegalArgumentException (api/open-tx-log *ingest-client* nil true))))))
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
  (t/is (nil? (api/latest-submitted-tx *api*)))

  (let [{:keys [crux.tx/tx-id] :as tx} (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
    (t/is (= {:crux.tx/tx-id tx-id}
             (api/latest-submitted-tx *api*))))

  (api/sync *api*)

  (t/is (= {:crux.db/id :foo} (api/entity (api/db *api*) :foo))))
