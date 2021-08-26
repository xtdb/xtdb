(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.every-api :as every-api :refer [*http-server-api* *node-type*]]
            [crux.fixtures.http-server :as fh]
            [crux.rdf :as rdf]
            [crux.tx :as tx]
            [crux.tx.event :as txe])
  (:import crux.api.NodeOutOfSyncException
           java.time.Duration
           java.util.Date
           org.eclipse.rdf4j.query.Binding
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository))

(t/use-fixtures :once every-api/with-embedded-kafka-cluster)
(t/use-fixtures :each every-api/with-each-api-implementation)

(defmacro with-dbs [[db db-args] & body]
  `(do
     (t/testing "with open-db"
       (with-open [~db (api/open-db ~@db-args)]
         ~@body))

     (t/testing "with db"
       (let [~db (api/db ~@db-args)]
         ~@body))

     (when-not (= *node-type* :remote)
       (t/testing "with speculative db"
         (let [~db (api/with-tx (api/db ~@db-args) [])]
           ~@body)))))

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:xt/id :ivan :name "Ivan"}]
    (t/testing "put"
      (let [tx (fix/submit+await-tx [[:xt/put content-ivan valid-time]])]
        (t/is (= {:xt/id :ivan, :name "Ivan"}
                 (api/entity (api/db *api* {:xt/valid-time valid-time, :xt/tx tx}) :ivan)))))

    (t/testing "delete"
      (let [delete-tx (api/submit-tx *api* [[:xt/delete :ivan valid-time]])]
        (api/await-tx *api* delete-tx)
        (t/is (nil? (api/entity (api/db *api* {:xt/valid-time valid-time, :xt/tx delete-tx}) :ivan)))))))

(t/deftest test-empty-db
  (let [empty-db (api/db *api*)]
    (t/is (nil? (api/sync *api* (Duration/ofSeconds 10))))
    (fix/submit+await-tx [[:xt/put {:xt/id :foo} #inst "2020"]])
    (t/is (= {:xt/id :foo} (api/entity (api/db *api*) :foo)))

    ;; TODO we don't currently distinguish between 'give me empty DB'
    ;; and 'give me latest tx-time' on the HTTP API when the tx-time QP is nil/missing
    (when-not (= *node-type* :remote)
      (t/is (nil? (api/entity empty-db :foo)))
      (t/is (empty? (api/entity-history empty-db :foo :asc))))))

(t/deftest test-status
  (t/is (= (merge {:crux.index/index-version 19}
                  (when (instance? xtdb.kafka.KafkaTxLog (:tx-log *api*))
                    {:crux.zk/zk-active? true}))
           (select-keys (api/status *api*) [:crux.index/index-version :crux.zk/zk-active?])))

  (let [submitted-tx (api/submit-tx *api* [[:xt/put {:xt/id :ivan :name "Ivan"}]])]
    (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
    (t/is (true? (api/tx-committed? *api* submitted-tx)))

    (let [status-map (api/status *api*)]
      (t/is (pos? (:crux.kv/estimate-num-keys status-map)))
      (t/is (= submitted-tx (api/latest-completed-tx *api*))))))

(t/deftest test-can-use-id-literals
  (let [id #xt/id "https://adam.com"
        doc {:xt/id id, :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:xt/put doc]])]
    (api/await-tx *api* submitted-tx nil)

    (t/is (= doc (api/entity (api/db *api*) id)))))

(t/deftest test-query
  (let [valid-time (Date.)
        submitted-tx (api/submit-tx *api* [[:xt/put {:xt/id :ivan :name "Ivan"} valid-time]
                                           [:xt/put {:xt/id :a :foo 1}]
                                           [:xt/put {:xt/id :b :foo 1}]
                                           [:xt/put {:xt/id :c :bar 2}]])]
    (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
    (t/is (true? (api/tx-committed? *api* submitted-tx)))

    (with-dbs [db (*api*)]
      (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                 '{:find [e]
                                   :where [[e :name "Ivan"]]})))

      (t/is (= #{} (api/q (api/db *api* #inst "1999")
                          '{:find [e]
                            :where [[e :name "Ivan"]]})))

      (t/testing "query :in args"
        (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                   '{:find [e]
                                     :in [n]
                                     :where [[e :name n]]}
                                   "Ivan"))))
      (t/testing "query ?pull [*]"
        (t/is (= #{[{:xt/id :ivan :name "Ivan"}]}
                 (api/q (api/db *api*)
                        '{:find [(pull e [*])]
                          :where [[e :name "Ivan"]]}))))

      (t/testing "query string"
        (t/is (= #{[:ivan]} (api/q db "{:find [e] :where [[e :name \"Ivan\"]]}"))))

      (t/testing "query vector"
        (t/is (= #{[:ivan]} (api/q db '[:find e
                                        :where [e :name "Ivan"]]))))

      (t/testing "malformed query"
        (t/is (thrown-with-msg? IllegalArgumentException
                                #"Query didn't match expected structure"
                                (api/q db '{:in [$ e]}))))

      (t/testing "query with streaming result"
        (with-open [res (api/open-q db '{:find [e]
                                         :where [[e :name "Ivan"]]})]
          (t/is (= '([:ivan])
                   (iterator-seq res)))))

      (t/testing "concurrent streaming queries"
        (with-open [q1 (api/open-q db '{:find [e]
                                        :where [[e :xt/id]]})
                    q2 (api/open-q db '{:find [e]
                                        :where [[e :foo]]})]
          (let [qq1 (iterator-seq q1)
                qq2 (iterator-seq q2)]
            (t/is (= '([[:a] [:a]] [[:b] [:b]] [[:c] :crux.test/nil] [[:ivan] :crux.test/nil])
                     (doall (for [i (range 10)
                                  :let [v1 (nth qq1 i :crux.test/nil)
                                        v2 (nth qq2 i :crux.test/nil)]
                                  :while (or (not= v1 :crux.test/nil)
                                             (not= v2 :crux.test/nil))]
                              [v1 v2]))))))))))

(t/deftest test-history
  (t/testing "transaction"
    (let [valid-time (Date.)
          submitted-tx (api/submit-tx *api* [[:xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]
      (api/await-tx *api* submitted-tx)
      (with-dbs [db (*api*)]
        (let [entity-tx (api/entity-tx db :ivan)
              ivan {:xt/id :ivan :name "Ivan"}
              ivan-crux-id (c/hash-doc ivan)]
          (t/is (= (merge submitted-tx
                          {:xt/id (c/new-id :ivan)
                           :xt/content-hash ivan-crux-id
                           :xt/valid-time valid-time})
                   entity-tx))
          (t/is (= [(dissoc entity-tx :xt/id)] (api/entity-history db :ivan :asc)))

          (t/is (nil? (api/entity-tx (api/db *api* #inst "1999") :ivan))))))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:xt/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (api/submit-tx *api* [[:xt/put doc]])]
    (api/await-tx *api* submitted-tx)
    (t/is (api/entity (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))
    (t/is (not-empty (api/entity-history (api/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"} :asc)))))

(t/deftest test-invalid-doc
  (t/is (thrown? IllegalArgumentException
                 (api/submit-tx *api* [[:xt/put {}]]))))

(t/deftest test-content-hash-invalid
  (let [valid-time (Date.)
        content-ivan {:xt/id :ivan :name "Ivan"}
        content-hash (str (c/new-id content-ivan))]
    (t/is (thrown-with-msg? IllegalArgumentException #"invalid doc"
                            (api/submit-tx *api* [[:xt/put content-hash valid-time]])))))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(t/deftest test-sparql
  (let [submitted-tx (api/submit-tx *api* [[:xt/put {:xt/id :ivan :name "Ivan"}]])]
    (api/await-tx *api* submitted-tx))

  (t/testing "SPARQL query"
    (when (bound? #'fh/*api-url*)
      (let [repo (SPARQLRepository. (str fh/*api-url* "/_crux/sparql"))]
        (try
          (.initialize repo)
          (with-open [conn (.getConnection repo)]
            (t/is (= #{[:ivan]} (execute-sparql conn "SELECT ?e WHERE { ?e <http://juxt.pro/crux/unqualified/name> \"Ivan\" }"))))
          (finally
            (.shutDown repo)))))))

(t/deftest test-adding-back-evicted-document
  (fix/submit+await-tx [[:xt/put {:xt/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo))

  (fix/submit+await-tx [[:xt/evict :foo]])
  (t/is (nil? (api/entity (api/db *api*) :foo)))

  (fix/submit+await-tx [[:xt/put {:xt/id :foo}]])
  (t/is (api/entity (api/db *api*) :foo)))

(t/deftest test-tx-log
  (let [valid-time (Date.)
        tx1 (fix/submit+await-tx [[:xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]

    (t/testing "tx-log"
      (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
        (let [result (iterator-seq tx-log-iterator)]
          (t/is (not (realized? result)))
          (t/is (= [(assoc tx1
                           :crux.tx.event/tx-events [[:xt/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"}) valid-time]])]
                   result))
          (t/is (realized? result))))

      (t/testing "with ops"
        (with-open [tx-log-iterator (api/open-tx-log *api* nil true)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (not (realized? result)))
            (t/is (= [(assoc tx1
                             :xt/tx-ops [[:xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]
                     result))
            (t/is (realized? result)))))

      (t/testing "from tx id - doesnt't include itself"
        (with-open [tx-log-iterator (api/open-tx-log *api* (:xt/tx-id tx1) false)]
          (t/is (empty? (iterator-seq tx-log-iterator)))))

      (t/testing "tx log skips failed transactions"
        (let [tx2 (fix/submit+await-tx [[:xt/match :ivan {:xt/id :ivan :name "Ivan2"}]
                                        [:xt/put {:xt/id :ivan :name "Ivan3"}]])]
          (t/is (false? (api/tx-committed? *api* tx2)))

          (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (= [(assoc tx1
                               :crux.tx.event/tx-events [[:xt/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"}) valid-time]])]
                       result))))

          (let [tx3 (fix/submit+await-tx [[:xt/match :ivan {:xt/id :ivan :name "Ivan"}]
                                          [:xt/put {:xt/id :ivan :name "Ivan3"}]])]
            (t/is (true? (api/tx-committed? *api* tx3)))
            (with-open [tx-log-iterator (api/open-tx-log *api* nil false)]
              (let [result (iterator-seq tx-log-iterator)]
                (t/is (= 2 (count result))))))))

      (t/testing "from tx id - doesn't include items <= `after-tx-id`"
        (with-open [tx-log-iterator (api/open-tx-log *api* (:xt/tx-id tx1) false)]
          (t/is (= 1 (count (iterator-seq tx-log-iterator))))))

      (t/testing "match includes eid"
        (let [tx (fix/submit+await-tx [[:xt/match :foo nil]])]
          (with-open [tx-log (api/open-tx-log *api* (dec (:xt/tx-id tx)) true)]
            (t/is (= [:xt/match (c/new-id :foo) (c/new-id nil)]
                     (-> (iterator-seq tx-log) first :xt/tx-ops first))))))

      ;; Intermittent failure on Kafka, see #1256
      (when-not (contains? #{:local-kafka :local-kafka-transit} *node-type*)
        (t/testing "tx fns return with-ops? correctly"
          (let [tx4 (fix/submit+await-tx [[:xt/put {:xt/id :jack :age 21}]
                                          [:xt/put {:xt/id :increment-age
                                                    :crux.db/fn '(fn [ctx eid]
                                                                   (let [db (crux.api/db ctx)
                                                                         entity (crux.api/entity db eid)]
                                                                     [[:xt/put (update entity :age inc)]]))}]
                                          [:xt/put {:xt/id :increment-age-2
                                                    :crux.db/fn '(fn [ctx eid]
                                                                   [[:xt/fn :increment-age eid]])}]
                                          [:xt/fn :increment-age-2 :jack]])]
            (t/is (true? (api/tx-committed? *api* tx4)))
            (with-open [tx-log-iterator (api/open-tx-log *api* nil true)]
              (let [tx-ops (-> tx-log-iterator iterator-seq last :xt/tx-ops)]
                (t/is (= [:xt/fn
                          (c/new-id :increment-age-2)
                          {:xt/tx-ops [[:xt/fn
                                        (c/new-id :increment-age)
                                        {:xt/tx-ops [[:xt/put {:xt/id :jack, :age 22}]]}]]}]
                         (last tx-ops)))))))))))

(t/deftest test-history-api
  (letfn [(submit-ivan [m valid-time]
            (let [doc (merge {:xt/id :ivan, :name "Ivan"} m)]
              (merge (fix/submit+await-tx [[:xt/put doc valid-time]])
                     {:xt/doc doc
                      :xt/valid-time valid-time
                      :xt/content-hash (c/hash-doc doc)})))]
    (let [v1 (submit-ivan {:version 1} #inst "2019-02-01")
          v2 (submit-ivan {:version 2} #inst "2019-02-02")
          v3 (submit-ivan {:version 3} #inst "2019-02-03")
          v2-corrected (submit-ivan {:version 2, :corrected? true} #inst "2019-02-02")]

      (with-dbs [db (*api* #inst "2019-02-03")]
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

      (with-dbs [db (*api* #inst "2019-02-02")]
        (t/is (= [v1 v2-corrected]
                 (api/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2-corrected v1]
                 (api/entity-history db :ivan :desc {:with-docs? true}))))

      (with-dbs [db (*api* #inst "2019-01-31")]
        (t/is (empty? (api/entity-history db :ivan :asc)))
        (t/is (empty? (api/entity-history db :ivan :desc)))

        (with-open [history-asc (api/open-entity-history db :ivan :asc)
                    history-desc (api/open-entity-history db :ivan :desc)]
          (t/is (empty? (iterator-seq history-asc)))
          (t/is (empty? (iterator-seq history-desc)))))

      (with-dbs [db (*api* #inst "2019-02-04")]
        (with-open [history-asc (api/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (api/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2-corrected v3]
                   (iterator-seq history-asc)))
          (t/is (= [v3 v2-corrected v1]
                   (iterator-seq history-desc)))))

      (with-dbs [db (*api* {:xt/valid-time #inst "2019-02-04", :xt/tx-time #inst "2019-01-31"})]
        (t/is (empty? (api/entity-history db :ivan :asc)))
        (t/is (empty? (api/entity-history db :ivan :desc))))

      (with-dbs [db (*api* {:xt/valid-time #inst "2019-02-02", :xt/tx v2})]
        (with-open [history-asc (api/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (api/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2]
                   (iterator-seq history-asc)))
          (t/is (= [v2 v1]
                   (iterator-seq history-desc)))))

      (with-dbs [db (*api* {:xt/valid-time #inst "2019-02-03", :xt/tx v2})]
        (t/is (= [v1 v2]
                 (api/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2 v1]
                 (api/entity-history db :ivan :desc {:with-docs? true})))))))

(t/deftest test-db-throws-if-future-tx-time-provided-546
  (let [{:keys [^Date xt/tx-time]} (fix/submit+await-tx [[:xt/put {:xt/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (api/db *api* the-future the-future)))))

(t/deftest test-db-is-a-snapshot
  (let [tx (fix/submit+await-tx [[:xt/put {:xt/id :foo, :count 0}]])
        db (api/db *api*)]
    (t/is (= tx (:xt/tx (api/db-basis db))))
    (t/is (= {:xt/id :foo, :count 0}
             (api/entity db :foo)))

    (fix/submit+await-tx [[:xt/put {:xt/id :foo, :count 1}]])

    (t/is (= {:xt/id :foo, :count 0}
             (api/entity db :foo)))))

(t/deftest test-latest-submitted-tx
  (t/is (nil? (api/latest-submitted-tx *api*)))
  (let [{:keys [xt/tx-id] :as tx} (api/submit-tx *api* [[:xt/put {:xt/id :foo}]])]
    (t/is (= {:xt/tx-id tx-id}
             (api/latest-submitted-tx *api*))))

  (api/sync *api*)

  (t/is (= {:xt/id :foo} (api/entity (api/db *api*) :foo))))

(t/deftest test-listen-for-indexed-txs
  (when-not (contains? (set t/*testing-contexts*) (str :remote))
    (let [!events (atom [])]
      (fix/submit+await-tx [[:xt/put {:xt/id :foo}]])

      (let [[bar-tx baz-tx] (with-open [_ (api/listen *api* {:xt/event-type :xt/indexed-tx
                                                             :with-tx-ops? true}
                                                      (fn [evt]
                                                        (swap! !events conj evt)))]

                              (let [bar-tx (fix/submit+await-tx [[:xt/put {:xt/id :bar}]])
                                    baz-tx (fix/submit+await-tx [[:xt/put {:xt/id :baz}]])]

                                (Thread/sleep 100)

                                [bar-tx baz-tx]))]

        (fix/submit+await-tx [[:xt/put {:xt/id :ivan}]])

        (Thread/sleep 100)

        (t/is (= [(merge {:xt/event-type :xt/indexed-tx,
                          :committed? true
                          :xt/tx-ops [[:xt/put {:xt/id :bar}]]}
                         bar-tx)
                  (merge {:xt/event-type :xt/indexed-tx,
                          :committed? true
                          :xt/tx-ops [[:xt/put {:xt/id :baz}]]}
                         baz-tx)]
                 @!events))))))

(t/deftest test-tx-fn-replacing-arg-docs-866
  (fix/submit+await-tx [[:xt/put {:xt/id :put-ivan
                                  :crux.db/fn '(fn [ctx doc]
                                                 [[:xt/put (assoc doc :xt/id :ivan)]])}]])

  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (t/testing "replaces args doc with resulting ops"
      (fix/submit+await-tx [[:xt/fn :put-ivan {:name "Ivan"}]])

      (t/is (= {:xt/id :ivan, :name "Ivan"}
               (api/entity (api/db *api*) :ivan)))

      (let [*server-api* (or *http-server-api* *api*)
            arg-doc-id (with-open [tx-log (db/open-tx-log (:tx-log *server-api*) nil)]
                         (-> (iterator-seq tx-log) last ::txe/tx-events first last))]

        (t/is (= {:crux.db.fn/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan, :name "Ivan"})]]}
                 (-> (db/fetch-docs (:document-store *server-api*) #{arg-doc-id})
                     (get arg-doc-id)
                     c/crux->xt
                     (dissoc :xt/id))))))))

(t/deftest test-await-tx
  (when-not (= *node-type* :remote)
    (t/testing "timeout outputs properly"
      (let [tx (api/submit-tx *api* (for [n (range 100)] [:xt/put {:xt/id (str "test-" n)}]))]
        (t/is
         (thrown-with-msg?
          java.util.concurrent.TimeoutException
          #"Timed out waiting for: #:xt"
          (api/await-tx *api* tx (Duration/ofNanos 1))))))))

(t/deftest missing-doc-halts-tx-ingestion
  (when-not (contains? #{:remote} *node-type*)
    (let [tx (db/submit-tx (:tx-log *api*) [[:crux.tx/put (c/new-id :foo) (c/new-id {:xt/id :foo})]])]
      (t/is (thrown-with-msg?
             IllegalStateException
             #"missing docs"
             (try
               (api/await-tx *api* tx (Duration/ofMillis 5000))
               (catch Exception e
                 (throw (.getCause e)))))))))

(t/deftest round-trips-clobs
  ;; ensure that anyone changing this also checks this test
  (t/is (= 224 @#'c/max-value-index-length))

  (let [clob (pr-str (range 1000))
        clob-doc {:xt/id :clob, :clob clob}]
    (fix/submit+await-tx [[:xt/put clob-doc]])

    (let [db (api/db *api*)]
      (t/is (= #{[clob]}
               (api/q db '{:find [?clob]
                           :where [[?e :clob ?clob]]})))

      (t/is (= #{[clob-doc]}
               (api/q db '{:find [(pull ?e [*])]
                           :where [[?e :clob]]})))

      (t/is (= clob-doc
               (api/entity db :clob))))))
