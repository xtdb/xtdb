(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.bootstrap.standalone]
            [crux.codec :as c]
            [crux.fixtures :as f]
            [crux.rdf :as rdf])
  (:import clojure.lang.LazySeq
           java.util.Date
           java.time.Duration
           crux.bootstrap.standalone.StandaloneSystem
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.query.Binding))

(t/use-fixtures :once f/with-embedded-kafka-cluster)
(t/use-fixtures :each f/with-each-api-implementation)

(declare execute-sparql)

(t/deftest test-can-use-api-to-access-crux
  (t/testing "status"
    (t/is (= {:crux.zk/zk-active? (not (instance? StandaloneSystem f/*api*))
              :crux.kv/kv-backend "crux.kv.rocksdb.RocksKv"
              :crux.kv/estimate-num-keys 1
              :crux.index/index-version 4
              :crux.tx-log/consumer-state nil}
             (dissoc (.status f/*api*) :crux.kv/size :crux.version/version :crux.version/revision))))

  (t/testing "empty db"
    (t/is (.db f/*api*)))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:keys [crux.tx/tx-time
                  crux.tx/tx-id]
           :as submitted-tx} (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (true? (.hasSubmittedTxUpdatedEntity f/*api* submitted-tx :ivan)))
      (t/is (= tx-time (.sync f/*api* (Duration/ofMillis 1000))))
      (t/is (= tx-time (.sync f/*api* nil)))

      (let [status-map (.status f/*api*)]
        (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
        (cond
          (and (instance? StandaloneSystem f/*api*)
               (instance? crux.tx.EventTxLog (:tx-log f/*api*)))
          (t/is (= {:crux.tx/event-log {:lag 0 :next-offset (inc tx-id) :time tx-time}}
                   (:crux.tx-log/consumer-state status-map)))

          (and (instance? StandaloneSystem f/*api*)
               (instance? crux.tx.KvTxLog (:tx-log f/*api*)))
          (t/is (= {:crux.kv.topic-partition/tx-log-0 {:lag 0 :time tx-time}}
                   (:crux.tx-log/consumer-state status-map)))

          :else
          (let [tx-topic-key (keyword "crux.kafka.topic-partition"
                                      (str (get-in f/*local-node* [:options :tx-topic]) "-0"))
                doc-topic-key (keyword "crux.kafka.topic-partition"
                                       (str (get-in f/*local-node* [:options :doc-topic]) "-0"))]
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
        (t/is (= #{[:ivan]} (.q (.db f/*api*)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]})))
        (t/is (= #{} (.q (.db f/*api* #inst "1999") '{:find [e]
                                                      :where [[e :name "Ivan"]]})))

        (t/testing "query string"
          (t/is (= #{[:ivan]} (.q (.db f/*api*)
                                  "{:find [e] :where [[e :name \"Ivan\"]]}"))))

        (t/testing "query vector"
          (t/is (= #{[:ivan]} (.q (.db f/*api*) '[:find e
                                                  :where [e :name "Ivan"]]))))

        (t/testing "malformed query"
          (t/is (thrown-with-msg? Exception
                                  #"(status 400|Spec assertion failed)"
                                  (.q (.db f/*api*) '{:find [e]}))))

        (t/testing "query with streaming result"
          (let [db (.db f/*api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]})]
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (= '([:ivan]) result))
                (t/is (realized? result))))))

        (t/testing "SPARQL query"
          (when (bound? #'f/*api-url*)
            (let [repo (SPARQLRepository. (str f/*api-url* "/sparql"))]
              (try
                (.initialize repo)
                (with-open [conn (.getConnection repo)]
                  (t/is (= #{[:ivan]} (execute-sparql conn "SELECT ?e WHERE { ?e <http://juxt.pro/crux/unqualified/name> \"Ivan\" }"))))
                (finally
                  (.shutDown repo)))))))

      (t/testing "entity"
        (t/is (= {:crux.db/id :ivan :name "Ivan"} (.entity (.db f/*api*) :ivan)))
        (t/is (nil? (.entity (.db f/*api* #inst "1999") :ivan))))

      (t/testing "entity-tx, document and history"
        (let [entity-tx (.entityTx (.db f/*api*) :ivan)]
          (t/is (= (merge submitted-tx
                          {:crux.db/id (str (c/new-id :ivan))
                           :crux.db/content-hash (str (c/new-id {:crux.db/id :ivan :name "Ivan"}))
                           :crux.db/valid-time valid-time})
                   entity-tx))
          (t/is (= {:crux.db/id :ivan :name "Ivan"} (.document f/*api* (:crux.db/content-hash entity-tx))))
          (t/is (= [entity-tx] (.history f/*api* :ivan)))
          (t/is (= [entity-tx] (.historyRange f/*api* :ivan #inst "1990" #inst "1990" (:crux.tx/tx-time submitted-tx) (:crux.tx/tx-time submitted-tx))))

          (t/is (nil? (.document f/*api* (c/new-id :does-not-exist))))
          (t/is (nil? (.entityTx (.db f/*api* #inst "1999") :ivan)))))

      (t/testing "tx-log"
        (with-open [ctx (.newTxLogContext f/*api*)]
          (let [result (.txLog f/*api* ctx nil false)]
            (t/is (instance? LazySeq result))
            (t/is (not (realized? result)))
            (t/is (= [(assoc submitted-tx
                             :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))
            (t/is (realized? result))))

        (t/testing "with documents"
          (with-open [ctx (.newTxLogContext f/*api*)]
            (let [result (.txLog f/*api* ctx nil true)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                               :crux.tx/tx-ops [[:crux.tx/put (c/new-id :ivan) {:crux.db/id :ivan :name "Ivan"} valid-time]])]
                       result))
              (t/is (realized? result)))))

        (t/testing "from tx id"
          (with-open [ctx (.newTxLogContext f/*api*)]
            (let [result (.txLog f/*api* ctx (inc tx-id) false)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (empty? result))
              (t/is (realized? result)))))))))

(t/deftest test-document-bug-123
  (let [version-1-submitted-tx (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan" :version 1}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity f/*api* version-1-submitted-tx :ivan))))

  (let [version-2-submitted-tx (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan" :version 2}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity f/*api* version-2-submitted-tx :ivan))))

  (let [history (.history f/*api* :ivan)]
    (t/is (= 2 (count history)))
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
              {:crux.db/id :ivan :name "Ivan" :version 1}]
             (for [content-hash (map :crux.db/content-hash history)]
               (.document f/*api* content-hash))))))

(t/deftest test-db-history-api
  (let [version-1-submitted-tx (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan" :version 1} #inst "2019-02-01"]])
        version-2-submitted-tx (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan" :version 2} #inst "2019-02-02"]])
        version-3-submitted-tx (.submitTx f/*api* [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan" :version 3} #inst "2019-02-03"]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity f/*api* version-3-submitted-tx :ivan)))
    (let [version-2-corrected-submitted-tx (.submitTx f/*api* [[:crux.tx/put
                                                                :ivan
                                                                {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                                                                #inst "2019-02-02"]])]
      (t/is (true? (.hasSubmittedTxCorrectedEntity f/*api* version-2-corrected-submitted-tx #inst "2019-02-02" :ivan)))

      (let [history (.history f/*api* :ivan)]
        (t/is (= 4 (count history))))

      (let [db (.db f/*api* #inst "2019-02-03")]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}]
                   (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                    {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                    {:crux.db/id :ivan :name "Ivan" :version 1}]
                   (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-02-02")]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                    {:crux.db/id :ivan :name "Ivan" :version 3}]
                   (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                    {:crux.db/id :ivan :name "Ivan" :version 1}]
                   (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-01-31")]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 1}
                    {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                    {:crux.db/id :ivan :name "Ivan" :version 3}]
                   (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (empty? (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-02-04")]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 3}
                    {:crux.db/id :ivan :name "Ivan" :version 2 :corrected true}
                    {:crux.db/id :ivan :name "Ivan" :version 1}]
                   (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-02-04" #inst "2019-01-31")]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (empty? (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-02-02" (:crux.tx/tx-time version-2-submitted-tx))]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}]
                   (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                    {:crux.db/id :ivan :name "Ivan" :version 1}]
                   (map :crux.db/doc (.historyDescending db snapshot :ivan))))))

      (let [db (.db f/*api* #inst "2019-02-03" (:crux.tx/tx-time version-2-submitted-tx))]
        (with-open [snapshot (.newSnapshot db)]
          (t/is (empty? (map :crux.db/doc (.historyAscending db snapshot :ivan))))
          (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
                    {:crux.db/id :ivan :name "Ivan" :version 1}]
                   (map :crux.db/doc (.historyDescending db snapshot :ivan)))))))))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))
