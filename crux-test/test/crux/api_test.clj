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
            [crux.tx.event :as txe]
            [crux.fixtures :as f]
            [clojure.java.io :as io])
  (:import crux.api.NodeOutOfSyncException
           java.util.Date
           java.time.Duration
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.query.Binding))

(def ^:dynamic *http-server-api* nil)

(def api-implementations
  (-> {:local-standalone (t/join-fixtures [fix/with-standalone-topology fix/with-kv-dir fix/with-node])
       :remote (t/join-fixtures [fix/with-standalone-topology
                                 fix/with-kv-dir
                                 fh/with-http-server
                                 fix/with-node
                                 (fn [f] (binding [*http-server-api* *api*] (f)))
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

(def ^:dynamic *node-type*)

(defn- with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

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
  (t/is (= (merge {:crux.index/index-version 9}
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
                          {:crux.db/id (c/new-id :ivan)
                           :crux.db/content-hash ivan-crux-id
                           :crux.db/valid-time valid-time})
                   entity-tx))
          (t/is (= [(dissoc entity-tx :crux.db/id)] (api/entity-history db :ivan :asc)))

          (t/is (nil? (api/entity-tx (api/db *api* #inst "1999") :ivan))))))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:crux.db/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:crux.tx/put doc]])]
    (api/await-tx *api* submitted-tx)
    (t/is (api/entity (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))
    (t/is (not-empty (api/entity-history (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"} :asc)))))

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
      (t/is (= 2 (:name stats))))))

(t/deftest test-adding-back-evicted-document
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo))

  (fix/submit+await-tx [[:crux.tx/evict :foo]])
  (t/is (nil? (api/entity (api/db *api*) :foo)))

  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo)))

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

      (with-both-dbs [db (*api* #inst "2019-02-03")]
        (t/is (= [v1 v2-corrected v3]
                 (api/entity-history db :ivan :asc {:with-docs? true})))

        (t/is (= [v3 v2-corrected v1]
                 (api/entity-history db :ivan :desc {:with-docs? true})))

        (with-open [history-asc (api/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (api/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2-corrected v3]
                   (iterator-seq history-asc)))
          (t/is (= [v3 v2-corrected v1]
                   (iterator-seq history-desc)))))

      (with-both-dbs [db (*api* #inst "2019-02-02")]
        (t/is (= [v1 v2-corrected]
                 (api/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2-corrected v1]
                 (api/entity-history db :ivan :desc {:with-docs? true}))))

      (with-both-dbs [db (*api* #inst "2019-01-31")]
        (t/is (empty? (api/entity-history db :ivan :asc)))
        (t/is (empty? (api/entity-history db :ivan :desc)))

        (with-open [history-asc (api/open-entity-history db :ivan :asc)
                    history-desc (api/open-entity-history db :ivan :desc)]
          (t/is (empty? (iterator-seq history-asc)))
          (t/is (empty? (iterator-seq history-desc)))))

      (with-both-dbs [db (*api* #inst "2019-02-04")]
        (with-open [history-asc (api/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (api/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2-corrected v3]
                   (iterator-seq history-asc)))
          (t/is (= [v3 v2-corrected v1]
                   (iterator-seq history-desc)))))

      (with-both-dbs [db (*api* #inst "2019-02-04" #inst "2019-01-31")]
        (t/is (empty? (api/entity-history db :ivan :asc)))
        (t/is (empty? (api/entity-history db :ivan :desc))))

      (with-both-dbs [db (*api* #inst "2019-02-02" (:crux.tx/tx-time v2))]
        (with-open [history-asc (api/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (api/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2]
                   (iterator-seq history-asc)))
          (t/is (= [v2 v1]
                   (iterator-seq history-desc)))))

      (with-both-dbs [db (*api* #inst "2019-02-03" (:crux.tx/tx-time v2))]
        (t/is (= [v1 v2]
                 (api/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2 v1]
                 (api/entity-history db :ivan :desc {:with-docs? true})))))))

(t/deftest test-db-throws-if-future-tx-time-provided-546
  (let [{:keys [^Date crux.tx/tx-time]} (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (api/db *api* the-future the-future)))))

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

(t/deftest test-tx-fn-replacing-arg-docs-866
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put-ivan
                                       :crux.db/fn '(fn [ctx doc]
                                                      [[:crux.tx/put (assoc doc :crux.db/id :ivan)]])}]])

  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (let [*server-api* (or *http-server-api* *api*)]
      (t/testing "replaces args doc with resulting ops"
        (fix/submit+await-tx [[:crux.tx/fn :put-ivan {:name "Ivan"}]])

        (t/is (= {:crux.db/id :ivan, :name "Ivan"}
                 (api/entity (api/db *api*) :ivan)))

        (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *server-api*) nil)]
                           (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

          (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put :ivan (c/new-id {:crux.db/id :ivan, :name "Ivan"})]]}
                   (-> (db/fetch-docs (:document-store *server-api*) #{arg-doc-id})
                       (get arg-doc-id)
                       (dissoc :crux.db/id))))))

      (t/testing "nested tx-fn"
        (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put-bob-and-ivan
                                             :crux.db/fn '(fn [ctx bob ivan]
                                                            [[:crux.tx/put (assoc bob :crux.db/id :bob)]
                                                             [:crux.tx/fn :put-ivan ivan]])}]])

        (fix/submit+await-tx [[:crux.tx/fn :put-bob-and-ivan {:name "Bob"} {:name "Ivan2"}]])

        (t/is (= {:crux.db/id :ivan, :name "Ivan2"}
                 (api/entity (api/db *api*) :ivan)))

        (t/is (= {:crux.db/id :bob, :name "Bob"}
                 (api/entity (api/db *api*) :bob)))

        (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *server-api*) 1)]
                           (-> (iterator-seq tx-log) last ::txe/tx-events first last))
              arg-doc (-> (db/fetch-docs (:document-store *server-api*) #{arg-doc-id})
                          (get arg-doc-id))

              sub-arg-doc-id (-> arg-doc :crux.db.fn/tx-events second last)
              sub-arg-doc (-> (db/fetch-docs (:document-store *server-api*) #{sub-arg-doc-id})
                              (get sub-arg-doc-id))]

          (t/is (= {:crux.db/id (:crux.db/id arg-doc)
                    :crux.db.fn/tx-events [[:crux.tx/put :bob (c/new-id {:crux.db/id :bob, :name "Bob"})]
                                           [:crux.tx/fn :put-ivan sub-arg-doc-id]]}
                   arg-doc))

          (t/is (= {:crux.db/id (:crux.db/id sub-arg-doc)
                    :crux.db.fn/tx-events [[:crux.tx/put :ivan (c/new-id {:crux.db/id :ivan :name "Ivan2"})]]}
                   sub-arg-doc))))

      (t/testing "copes with args doc having been replaced"
        (let [sergei {:crux.db/id :sergei
                      :name "Sergei"}
              arg-doc {:crux.db/id :args
                       :crux.db.fn/tx-events [[:crux.tx/put :sergei (c/new-id sergei)]]}]
          (db/submit-docs (:document-store *server-api*)
                          {(c/new-id arg-doc) arg-doc
                           (c/new-id sergei) sergei})
          (let [tx @(db/submit-tx (:tx-log *server-api*) [[:crux.tx/fn :put-sergei (c/new-id arg-doc)]])]
            (api/await-tx *api* tx)

            (t/is (= sergei (api/entity (api/db *api*) :sergei))))))

      (t/testing "failed tx-fn"
        (fix/submit+await-tx [[:crux.tx/fn :put-petr {:name "Petr"}]])

        (t/is (nil? (api/entity (api/db *api*) :petr)))

        (let [arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *server-api*) nil)]
                           (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

          (t/is (= {:crux.db.fn/failed? true
                    :crux.db.fn/exception 'java.lang.NullPointerException
                    :crux.db.fn/message nil
                    :crux.db.fn/ex-data nil}
                   (-> (db/fetch-docs (:document-store *server-api*) #{arg-doc-id})
                       (get arg-doc-id)
                       (dissoc :crux.db/id)))))))))
