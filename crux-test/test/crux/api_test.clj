(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.standalone]
            [crux.fixtures.standalone :as fs]
            [crux.moberg]
            [crux.codec :as c]
            [crux.fixtures.api :refer [*api*] :as apif]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.kafka :as fk :refer [*ingest-client*]]
            crux.jdbc
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.http-server :as fh]
            [crux.fixtures.kafka :as kf]
            [crux.rdf :as rdf]
            [crux.api :as api]
            [crux.fixtures.api :as apif])
  (:import clojure.lang.LazySeq
           java.util.Date
           java.time.Duration
           crux.moberg.MobergTxLog
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.query.Binding))

(defn- with-each-api-implementation [f]
  (t/testing "Local API ClusterNode"
    (kf/with-cluster-node-opts f))
  (t/testing "Local API StandaloneNode"
    (fs/with-standalone-node f))
  (t/testing "JDBC Node"
    (fj/with-jdbc-node :h2 f))
  (t/testing "Remote API"
    (fn [f]
      (fh/with-http-server
        (fn [f]
          (kf/with-cluster-node-opts f))))))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each with-each-api-implementation kvf/with-kv-dir apif/with-node)

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
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))))

(t/deftest test-can-use-crux-ids
  (let [id #crux/id :https://adam.com
        doc {:crux.db/id id, :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) id))))

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}]

    (t/testing "put works with no id"
      (t/is
       (let [submitted-tx (.submitTx *api* [[:crux.tx/put content-ivan valid-time]])]
         (.db *api* valid-time (:crux.tx/tx-time submitted-tx)))))

    (t/testing "Delete works with id"
      (t/is (.submitTx *api* [[:crux.tx/delete :ivan]])))))

(t/deftest test-can-use-api-to-access-crux
  (t/testing "status"
    (t/is (= (merge {:crux.index/index-version 4}
                    (when (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
                      {:crux.zk/zk-active? true}))
             (dissoc (.status *api*)
                     :crux.kv/kv-store
                     :crux.kv/estimate-num-keys
                     :crux.tx-log/consumer-state
                     :crux.kv/size
                     :crux.version/version
                     :crux.version/revision))))

  (t/testing "empty db"
    (t/is (.db *api*)))

  (t/testing "syncing empty db"
    (t/is (nil? (.sync *api* (Duration/ofSeconds 10)))))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:keys [crux.tx/tx-time
                  crux.tx/tx-id]
           :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
      (t/is (= tx-time (.sync *api* (:crux.tx/tx-time submitted-tx) nil)))

      (let [status-map (.status *api*)]
        (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
        (cond
          (instance? crux.moberg.MobergTxLog (:tx-log *api*))
          (t/is (= {:crux.tx/event-log {:lag 0 :next-offset (inc tx-id) :time tx-time}}
                   (:crux.tx-log/consumer-state status-map)))

          (instance? crux.jdbc.JdbcTxLog (:tx-log *api*))
          (t/is (= {:crux.tx/event-log {:lag 0 :next-offset (inc tx-id) :time tx-time}}
                   (:crux.tx-log/consumer-state status-map)))

          :else
          (let [tx-topic-key (keyword "crux.kafka.topic-partition" (str fk/*tx-topic* "-0"))
                doc-topic-key (keyword "crux.kafka.topic-partition" (str fk/*doc-topic* "-0"))]
            (t/is (= {:lag 0
                      :next-offset 1
                      :time tx-time}
                     (get-in status-map [:crux.tx-log/consumer-state tx-topic-key])))
            (t/is (= {:lag 0
                      :next-offset 1}
                     (-> status-map
                         (get-in [:crux.tx-log/consumer-state doc-topic-key])
                         (dissoc :time)))))))

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
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (= '([:ivan]) result))
                (t/is (realized? result))))))

        (t/testing "query returning full results"
          (let [db (.db *api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]
                                             :full-results? true})]
                (t/is (instance? LazySeq result))
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
          (t/is (= [entity-tx] (.historyRange *api* :ivan #inst "1990" #inst "1990" (:crux.tx/tx-time submitted-tx) (:crux.tx/tx-time submitted-tx))))

          (t/is (nil? (.document *api* (c/new-id :does-not-exist))))
          (t/is (nil? (.entityTx (.db *api* #inst "1999") :ivan)))))

      (t/testing "tx-log"
        (with-open [ctx (.newTxLogContext *api*)]
          (let [result (.txLog *api* ctx nil false)]
            (t/is (instance? LazySeq result))
            (t/is (not (realized? result)))
            (t/is (= [(assoc submitted-tx
                             :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))
            (t/is (realized? result))))

        (t/testing "with ops"
          (with-open [ctx (.newTxLogContext *api*)]
            (let [result (.txLog *api* ctx nil true)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                               :crux.api/tx-ops [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
                       result))
              (t/is (realized? result)))))

        (t/testing "from tx id"
          (with-open [ctx (.newTxLogContext *api*)]
            (let [result (.txLog *api* ctx (inc tx-id) false)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (empty? result))
              (t/is (realized? result))))))

      (t/testing "statistics"
        (let [stats (.attributeStats *api*)]
          (t/is (= 1 (:name stats))))

        (t/testing "updated"
          (let [valid-time (Date.)
                {:keys [crux.tx/tx-time
                        crux.tx/tx-id]
                 :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan2"} valid-time]])]
            (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
            (t/is (= tx-time (.sync *api* (:crux.tx/tx-time submitted-tx) nil)))
            (t/is (= tx-time (.sync *api* nil))))

          (let [stats (.attributeStats *api*)]
            (t/is (= 2 (:name stats)))))

        (t/testing "reflect evicted documents"
          (let [valid-time (Date.)
                {:keys [crux.tx/tx-time
                        crux.tx/tx-id]
                 :as submitted-tx} (.submitTx *api* [[:crux.tx/evict :ivan]])]
            (t/is (.sync *api* tx-time nil))

            ;; actual removal of the document happens asynchronously after
            ;; the transaction has been processed so waiting on the
            ;; submitted transaction time is not enough
            (while (.entity (.db *api*) :ivan)
              (assert (< (- (.getTime (Date.)) (.getTime valid-time)) 4000))
              (Thread/sleep 500))

            (let [stats (.attributeStats *api*)]
              (t/is (= 0 (:name stats))))))

        (t/testing "Add back evicted document"
          (assert (not (.entity (.db *api*) :ivan)))
          (let [valid-time (Date.)
                {:keys [crux.tx/tx-time]
                 :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
            (t/is (.sync *api* tx-time nil))
            (t/is (.entity (.db *api*) :ivan))))))))

(t/deftest test-document-bug-123
  (let [version-1-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity *api* version-1-submitted-tx :ivan))))

  (let [version-2-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity *api* version-2-submitted-tx :ivan))))

  (let [history (.history *api* :ivan)]
    (t/is (= 2 (count history)))
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
              {:crux.db/id :ivan :name "Ivan" :version 1}]
             (for [content-hash (map :crux.db/content-hash history)]
               (.document *api* content-hash))))))

(t/deftest test-tx-log-skips-failed-transactions
  (let [valid-time (Date.)
        submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
    (let [version-2-submitted-tx (.submitTx *api* [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan2"} {:crux.db/id :ivan :name "Ivan3"}]])]
      (t/is (false? (.hasSubmittedTxUpdatedEntity *api* version-2-submitted-tx :ivan)))
      (with-open [ctx (.newTxLogContext *api*)]
        (let [result (.txLog *api* ctx nil false)]
          (t/is (= [(assoc submitted-tx
                           :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                   result))))

      (let [version-3-submitted-tx (.submitTx *api* [[:crux.tx/cas {:crux.db/id :ivan :name "Ivan"} {:crux.db/id :ivan :name "Ivan3"}]])]
        (t/is (true? (.hasSubmittedTxUpdatedEntity *api* version-3-submitted-tx :ivan)))
        (with-open [ctx (.newTxLogContext *api*)]
          (let [result (.txLog *api* ctx nil false)]
            (t/is (= 2 (count result)))))))))

(t/deftest test-db-history-api
  (let [version-1-submitted-tx-time (.sync *api* (:crux.tx/tx-time (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1} #inst "2019-02-01"]])) nil)
        version-2-submitted-tx-time (.sync *api* (:crux.tx/tx-time (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2} #inst "2019-02-02"]])) nil)
        version-3-submitted-tx-time (.sync *api* (:crux.tx/tx-time (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 3} #inst "2019-02-03"]])) nil)
        version-2-corrected-submitted-tx-time (.sync *api* (:crux.tx/tx-time (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true} #inst "2019-02-02"]])) nil)]

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
  (if (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
    (kf/with-ingest-client
      (fn []
        (let [submitted-tx @(.submitTxAsync *ingest-client* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
          (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
          (t/is (= #{[:ivan]} (.q (.db *api*)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]})))

          (with-open [ctx (.newTxLogContext *ingest-client*)]
            (let [result (.txLog *ingest-client* ctx nil false)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                               :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"})]])]
                       result))
              (t/is (realized? result))))

          (with-open [ctx (.newTxLogContext *ingest-client*)]
            (t/is (thrown? IllegalArgumentException (.txLog *ingest-client* ctx nil true)))))))
    (t/is true)))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))
