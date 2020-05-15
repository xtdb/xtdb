(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.fixtures :refer [*api*] :as fix]
            [crux.fixtures.kafka :as fk]
            crux.jdbc
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.http-server :as fh]
            [crux.rdf :as rdf]
            [crux.api :as api]
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
  (-> {:local-standalone (t/join-fixtures [fix/with-standalone-topology fix/with-kv-dir fix/with-node])
       :remote (t/join-fixtures [fix/with-standalone-topology
                                 fix/with-kv-dir
                                 fh/with-http-server
                                 fix/with-node
                                 fh/with-http-client])
       :h2 (t/join-fixtures [#(fj/with-jdbc-node :h2 %) fix/with-kv-dir fix/with-node])
       :sqlite (t/join-fixtures [#(fj/with-jdbc-node :sqlite %) fix/with-kv-dir fix/with-node])
       :local-kafka (-> (t/join-fixtures [fk/with-cluster-node-opts fix/with-kv-dir fix/with-node])
                        (with-meta {::embedded-kafka? true}))
       :kafka+remote-doc-store (-> (t/join-fixtures [fk/with-cluster-node-opts
                                                     fix/with-standalone-doc-store
                                                     fix/with-kv-dir
                                                     fix/with-node])
                                   (with-meta {::embedded-kafka? true}))}
      #_(select-keys [:local-standalone])
      #_(select-keys [:local-standalone :remote])
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

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}]
    (t/testing "put"
      (let [{::tx/keys [tx-time] :as tx} (fix/submit+await-tx [[:crux.tx/put content-ivan valid-time]])]
        (t/is (= {:crux.db/id :ivan :name "Ivan"}
                 (api/entity (api/db *api* valid-time tx-time) :ivan)))))

    (t/testing "delete"
      (let [{::tx/keys [tx-time] :as delete-tx} (api/submit-tx *api* [[:crux.tx/delete :ivan valid-time]])]
        (api/await-tx *api* delete-tx)
        (t/is (nil? (api/entity (api/db *api* valid-time tx-time) :ivan)))))))

(t/deftest test-empty-db
  (t/is (api/db *api*))

  (t/testing "syncing empty db"
    (t/is (nil? (api/sync *api* (Duration/ofSeconds 10))))))

(t/deftest test-status
  (t/is (= (merge {:crux.index/index-version 6}
                  (when (instance? crux.kafka.KafkaTxLog (:tx-log *api*))
                    {:crux.zk/zk-active? true}))
           (select-keys (api/status *api*) [:crux.index/index-version :crux.zk/zk-active?])))

  (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
    (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
    (t/is (true? (api/tx-committed? *api* submitted-tx)))

    (let [status-map (api/status *api*)]
      (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
      (t/is (= submitted-tx (api/latest-completed-tx *api*))))))

(t/deftest test-can-use-crux-ids
  (let [id #crux/id :https://adam.com
        doc {:crux.db/id id, :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.awaitTx *api* submitted-tx nil)
    (t/is (.entity (.db *api*) id))))

(t/deftest test-query
  (let [valid-time (Date.)
        submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
    (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
    (t/is (true? (api/tx-committed? *api* submitted-tx)))

    (t/testing "query"
      (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                 '{:find [e]
                                   :where [[e :name "Ivan"]]})))
      (t/is (= #{} (api/q (api/db *api* #inst "1999")
                          '{:find [e]
                            :where [[e :name "Ivan"]]})))

      (with-both-dbs [db (*api*)]
        (t/testing "query string"
          (t/is (= #{[:ivan]} (api/q db "{:find [e] :where [[e :name \"Ivan\"]]}"))))

        (t/testing "query vector"
          (t/is (= #{[:ivan]} (api/q db '[:find e
                                          :where [e :name "Ivan"]]))))

        (t/testing "malformed query"
          (t/is (thrown-with-msg? Exception
                                  #"(status 400|Spec assertion failed)"
                                  (api/q db '{:find [e]}))))

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
                     (iterator-seq res)))))))))

(t/deftest test-history
  (t/testing "transaction"
    (let [valid-time (Date.)
          {::tx/keys [tx-time tx-id] :as submitted-tx} (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (api/await-tx *api* submitted-tx)
      (with-both-dbs [db (*api*)]
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
          (t/is (nil? (api/entity-tx (api/db *api* #inst "1999") :ivan))))))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:crux.db/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:crux.tx/put doc]])]
    (api/await-tx *api* submitted-tx)
    (t/is (api/entity (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))
    (t/is (not-empty (api/history *api* {:user "Xwop1A7Xog4nD6AfhZaPgg"})))))

(t/deftest test-invalid-doc
  (t/is (thrown? IllegalArgumentException
                 (api/submit-tx *api* [[:crux.tx/put {}]]))))

(t/deftest test-content-hash-invalid
  (let [valid-time (Date.)
        content-ivan {:crux.db/id :ivan :name "Ivan"}
        content-hash (str (c/new-id content-ivan))]
    (t/is (thrown-with-msg? IllegalArgumentException #"invalid doc"
                            (.submitTx *api* [[:crux.tx/put content-hash valid-time]])))))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

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
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo))

  (fix/submit+await-tx [[:crux.tx/evict :foo]])
  (t/is (nil? (api/entity (api/db *api*) :foo)))

  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
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
        tx1 (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]

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
      (let [tx2 (fix/submit+await-tx [[:crux.tx/match :ivan {:crux.db/id :ivan :name "Ivan2"}]
                                       [:crux.tx/put {:crux.db/id :ivan :name "Ivan3"}]])]
        (t/is (false? (api/tx-committed? *api* tx2)))

        (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (= [(assoc tx1
                        :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                     result))))

        (let [tx3 (fix/submit+await-tx [[:crux.tx/match :ivan {:crux.db/id :ivan :name "Ivan"}]
                                         [:crux.tx/put {:crux.db/id :ivan :name "Ivan3"}]])]
          (t/is (true? (api/tx-committed? *api* tx3)))
          (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (= 2 (count result))))))))))

(t/deftest test-history-api
  (letfn [(submit-ivan [m valid-time]
            (let [doc (merge {:crux.db/id :ivan, :name "Ivan"} m)]
              (merge (fix/submit+await-tx [[:crux.tx/put doc valid-time]])
                     {:crux.db/doc doc
                      :crux.db/valid-time valid-time
                      :crux.db/content-hash (c/new-id doc)})))]
    (let [v1 (submit-ivan {:version 1} #inst "2019-02-01")
          v2 (submit-ivan {:version 2} #inst "2019-02-02")
          v3 (submit-ivan {:version 3} #inst "2019-02-03")
          v2-corrected (submit-ivan {:version 2, :corrected? true} #inst "2019-02-02")]

      (with-open [history-asc (api/open-entity-history (api/db *api*) :ivan :asc
                                                       {:start {:crux.db/valid-time #inst "2019-02-03"}
                                                        :with-docs? true})]
        (t/is (= [v3] (iterator-seq history-asc))))

      (with-open [history-desc (api/open-entity-history (api/db *api*) :ivan :desc
                                                        {:start {:crux.db/valid-time #inst "2019-02-03"}
                                                         :with-docs? true})]
        (t/is (= [v3 v2-corrected v1] (iterator-seq history-desc)))))))

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

(t/deftest test-db-throws-if-future-tx-time-provided-546
  (let [{:keys [^Date crux.tx/tx-time]} (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (api/db *api* the-future the-future)))))

(t/deftest test-history-range-throws-if-future-tx-time-provided-827
  (let [{:keys [^Date crux.tx/tx-time]} (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (api/history-range *api* :foo nil nil nil the-future)))))

(t/deftest test-latest-submitted-tx
  (t/is (nil? (.latestSubmittedTx *api*)))

  (let [{:keys [crux.tx/tx-id] :as tx} (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
    (t/is (= {:crux.tx/tx-id tx-id}
             (.latestSubmittedTx *api*))))

  (api/sync *api*)

  (t/is (= {:crux.db/id :foo} (api/entity (api/db *api*) :foo))))

(t/deftest test-listen-for-indexed-txs
  (when-not (contains? (set t/*testing-contexts*) (str :remote))
    (let [!events (atom [])]
      (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])

      (let [[bar-tx baz-tx] (with-open [_ (api/listen *api* {:crux/event-type :crux/indexed-tx
                                                             :with-tx-ops? true}
                                                      (fn [evt]
                                                        (swap! !events conj evt)))]

                              (let [bar-tx (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :bar}]])
                                    baz-tx (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :baz}]])]

                                (Thread/sleep 100)

                                [bar-tx baz-tx]))]

        (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan}]])

        (Thread/sleep 100)

        (t/is (= [(merge {:crux/event-type :crux/indexed-tx,
                          :committed? true
                          :crux/tx-ops [[:crux.tx/put {:crux.db/id :bar}]]}
                         bar-tx)
                  (merge {:crux/event-type :crux/indexed-tx,
                          :committed? true
                          :crux/tx-ops [[:crux.tx/put {:crux.db/id :baz}]]}
                         baz-tx)]
                 @!events))))))
