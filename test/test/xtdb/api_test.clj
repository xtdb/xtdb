(ns xtdb.api-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.every-api :as every-api :refer [*http-server-api* *node-type*]]
            [xtdb.fixtures.http-server :as fh]
            [xtdb.rdf :as rdf]
            [xtdb.tx :as tx]
            [xtdb.tx.event :as txe])
  (:import xtdb.api.NodeOutOfSyncException
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
       (with-open [~db (xt/open-db ~@db-args)]
         ~@body))

     (t/testing "with db"
       (let [~db (xt/db ~@db-args)]
         ~@body))

     (when-not (= *node-type* :remote)
       (t/testing "with speculative db"
         (let [~db (xt/with-tx (xt/db ~@db-args) [])]
           ~@body)))))

(t/deftest test-single-id
  (let [valid-time (Date.)
        content-ivan {:xt/id :ivan :name "Ivan"}]
    (t/testing "put"
      (let [tx (fix/submit+await-tx [[::xt/put content-ivan valid-time]])]
        (t/is (= {:xt/id :ivan, :name "Ivan"}
                 (xt/entity (xt/db *api* {::xt/valid-time valid-time, ::xt/tx tx}) :ivan)))))

    (t/testing "delete"
      (let [delete-tx (xt/submit-tx *api* [[::xt/delete :ivan valid-time]])]
        (xt/await-tx *api* delete-tx)
        (t/is (nil? (xt/entity (xt/db *api* {::xt/valid-time valid-time, ::xt/tx delete-tx}) :ivan)))))))

(t/deftest test-empty-db
  (let [empty-db (xt/db *api*)]
    (t/is (nil? (xt/sync *api* (Duration/ofSeconds 10))))
    (fix/submit+await-tx [[::xt/put {:xt/id :foo} #inst "2020"]])
    (t/is (= {:xt/id :foo} (xt/entity (xt/db *api*) :foo)))

    ;; TODO we don't currently distinguish between 'give me empty DB'
    ;; and 'give me latest tx-time' on the HTTP API when the tx-time QP is nil/missing
    (when-not (= *node-type* :remote)
      (t/is (nil? (xt/entity empty-db :foo)))
      (t/is (empty? (xt/entity-history empty-db :foo :asc))))))

(t/deftest test-status
  (t/is (= (merge {:xtdb.index/index-version 19}
                  (when (instance? xtdb.kafka.KafkaTxLog (:tx-log *api*))
                    {:xtdb.zk/zk-active? true}))
           (select-keys (xt/status *api*) [:xtdb.index/index-version :xtdb.zk/zk-active?])))

  (let [submitted-tx (xt/submit-tx *api* [[::xt/put {:xt/id :ivan :name "Ivan"}]])]
    (t/is (= submitted-tx (xt/await-tx *api* submitted-tx)))
    (t/is (true? (xt/tx-committed? *api* submitted-tx)))

    (let [status-map (xt/status *api*)]
      (t/is (pos? (:xtdb.kv/estimate-num-keys status-map)))
      (t/is (= submitted-tx (xt/latest-completed-tx *api*))))))

(t/deftest test-can-use-id-literals
  (let [id #xtdb/id "https://adam.com"
        doc {:xt/id id, :name "Adam"}
        submitted-tx (xt/submit-tx *api* [[::xt/put doc]])]
    (xt/await-tx *api* submitted-tx nil)

    (t/is (= doc (xt/entity (xt/db *api*) id)))))

(t/deftest test-query
  (let [valid-time (Date.)
        submitted-tx (xt/submit-tx *api* [[::xt/put {:xt/id :ivan :name "Ivan"} valid-time]
                                          [::xt/put {:xt/id :a :foo 1}]
                                          [::xt/put {:xt/id :b :foo 1}]
                                          [::xt/put {:xt/id :c :bar 2}]])]
    (t/is (= submitted-tx (xt/await-tx *api* submitted-tx)))
    (t/is (true? (xt/tx-committed? *api* submitted-tx)))

    (with-dbs [db (*api*)]
      (t/is (= #{[:ivan]} (xt/q (xt/db *api*)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]})))

      (t/is (= #{} (xt/q (xt/db *api* #inst "1999")
                         '{:find [e]
                           :where [[e :name "Ivan"]]})))

      (t/testing "query :in args"
        (t/is (= #{[:ivan]} (xt/q (xt/db *api*)
                                  '{:find [e]
                                    :in [n]
                                    :where [[e :name n]]}
                                  "Ivan"))))
      (t/testing "query ?pull [*]"
        (t/is (= #{[{:xt/id :ivan :name "Ivan"}]}
                 (xt/q (xt/db *api*)
                       '{:find [(pull e [*])]
                         :where [[e :name "Ivan"]]}))))

      (t/testing "query string"
        (t/is (= #{[:ivan]} (xt/q db "{:find [e] :where [[e :name \"Ivan\"]]}"))))

      (t/testing "query vector"
        (t/is (= #{[:ivan]} (xt/q db '[:find e
                                       :where [e :name "Ivan"]]))))

      (t/testing "malformed query"
        (t/is (thrown-with-msg? IllegalArgumentException
                                #"Query didn't match expected structure"
                                (xt/q db '{:in [$ e]}))))

      (t/testing "query with streaming result"
        (with-open [res (xt/open-q db '{:find [e]
                                        :where [[e :name "Ivan"]]})]
          (t/is (= '([:ivan])
                   (iterator-seq res)))))

      (t/testing "concurrent streaming queries"
        (with-open [q1 (xt/open-q db '{:find [e]
                                       :where [[e :xt/id]]})
                    q2 (xt/open-q db '{:find [e]
                                       :where [[e :foo]]})]
          (let [qq1 (iterator-seq q1)
                qq2 (iterator-seq q2)]
            (t/is (= '([[:a] [:a]] [[:b] [:b]] [[:c] :xtdb.test/nil] [[:ivan] :xtdb.test/nil])
                     (doall (for [i (range 10)
                                  :let [v1 (nth qq1 i :xtdb.test/nil)
                                        v2 (nth qq2 i :xtdb.test/nil)]
                                  :while (or (not= v1 :xtdb.test/nil)
                                             (not= v2 :xtdb.test/nil))]
                              [v1 v2]))))))))))

(t/deftest test-history
  (t/testing "transaction"
    (let [valid-time (Date.)
          submitted-tx (xt/submit-tx *api* [[::xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]
      (xt/await-tx *api* submitted-tx)
      (with-dbs [db (*api*)]
        (let [entity-tx (xt/entity-tx db :ivan)
              ivan {:xt/id :ivan :name "Ivan"}
              ivan-xtdb-id (c/hash-doc ivan)]
          (t/is (= (merge submitted-tx
                          {:xt/id (c/new-id :ivan)
                           ::xt/content-hash ivan-xtdb-id
                           ::xt/valid-time valid-time})
                   entity-tx))
          (t/is (= [(dissoc entity-tx :xt/id)] (xt/entity-history db :ivan :asc)))

          (t/is (nil? (xt/entity-tx (xt/db *api* #inst "1999") :ivan))))))))

(t/deftest test-can-write-entity-using-map-as-id
  (let [doc {:xt/id {:user "Xwop1A7Xog4nD6AfhZaPgg"} :name "Adam"}
        submitted-tx (xt/submit-tx *api* [[::xt/put doc]])]
    (xt/await-tx *api* submitted-tx)
    (t/is (xt/entity (xt/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"}))
    (t/is (not-empty (xt/entity-history (xt/db *api*) {:user "Xwop1A7Xog4nD6AfhZaPgg"} :asc)))))

(t/deftest test-invalid-doc
  (t/is (thrown? IllegalArgumentException
                 (xt/submit-tx *api* [[::xt/put {}]]))))

(t/deftest test-content-hash-invalid
  (let [valid-time (Date.)
        content-ivan {:xt/id :ivan :name "Ivan"}
        content-hash (str (c/new-id content-ivan))]
    (t/is (thrown-with-msg? IllegalArgumentException #"invalid doc"
                            (xt/submit-tx *api* [[::xt/put content-hash valid-time]])))))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(t/deftest test-sparql
  (let [submitted-tx (xt/submit-tx *api* [[::xt/put {:xt/id :ivan :name "Ivan"}]])]
    (xt/await-tx *api* submitted-tx))

  (t/testing "SPARQL query"
    (when (bound? #'fh/*api-url*)
      (let [repo (SPARQLRepository. (str fh/*api-url* "/_xtdb/sparql"))]
        (try
          (.initialize repo)
          (with-open [conn (.getConnection repo)]
            (t/is (= #{[:ivan]} (execute-sparql conn "SELECT ?e WHERE { ?e <http://xtdb.com/unqualified/name> \"Ivan\" }"))))
          (finally
            (.shutDown repo)))))))

(t/deftest test-adding-back-evicted-document
  (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])
  (t/is (xt/entity (xt/db *api*) :foo))

  (fix/submit+await-tx [[::xt/evict :foo]])
  (t/is (nil? (xt/entity (xt/db *api*) :foo)))

  (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])
  (t/is (xt/entity (xt/db *api*) :foo)))

(t/deftest test-tx-log
  (let [valid-time (Date.)
        tx1 (fix/submit+await-tx [[::xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]

    (t/testing "tx-log"
      (with-open [tx-log-iterator (xt/open-tx-log *api* nil false)]
        (let [result (iterator-seq tx-log-iterator)]
          (t/is (not (realized? result)))
          (t/is (= [(assoc tx1
                           :xtdb.tx.event/tx-events [[::xt/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"}) valid-time]])]
                   result))
          (t/is (realized? result))))

      (t/testing "with ops"
        (with-open [tx-log-iterator (xt/open-tx-log *api* nil true)]
          (let [result (iterator-seq tx-log-iterator)]
            (t/is (not (realized? result)))
            (t/is (= [(assoc tx1
                             ::xt/tx-ops [[::xt/put {:xt/id :ivan :name "Ivan"} valid-time]])]
                     result))
            (t/is (realized? result)))))

      (t/testing "from tx id - doesnt't include itself"
        (with-open [tx-log-iterator (xt/open-tx-log *api* (::xt/tx-id tx1) false)]
          (t/is (empty? (iterator-seq tx-log-iterator)))))

      (t/testing "tx log skips failed transactions"
        (let [tx2 (fix/submit+await-tx [[::xt/match :ivan {:xt/id :ivan :name "Ivan2"}]
                                        [::xt/put {:xt/id :ivan :name "Ivan3"}]])]
          (t/is (false? (xt/tx-committed? *api* tx2)))

          (with-open [tx-log-iterator (xt/open-tx-log *api* nil false)]
            (let [result (iterator-seq tx-log-iterator)]
              (t/is (= [(assoc tx1
                               :xtdb.tx.event/tx-events [[::xt/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"}) valid-time]])]
                       result))))

          (let [tx3 (fix/submit+await-tx [[::xt/match :ivan {:xt/id :ivan :name "Ivan"}]
                                          [::xt/put {:xt/id :ivan :name "Ivan3"}]])]
            (t/is (true? (xt/tx-committed? *api* tx3)))
            (with-open [tx-log-iterator (xt/open-tx-log *api* nil false)]
              (let [result (iterator-seq tx-log-iterator)]
                (t/is (= 2 (count result))))))))

      (t/testing "from tx id - doesn't include items <= `after-tx-id`"
        (with-open [tx-log-iterator (xt/open-tx-log *api* (::xt/tx-id tx1) false)]
          (t/is (= 1 (count (iterator-seq tx-log-iterator))))))

      (t/testing "match includes eid"
        (let [tx (fix/submit+await-tx [[::xt/match :foo nil]])]
          (with-open [tx-log (xt/open-tx-log *api* (dec (::xt/tx-id tx)) true)]
            (t/is (= [::xt/match (c/new-id :foo) (c/new-id nil)]
                     (-> (iterator-seq tx-log) first ::xt/tx-ops first))))))

      ;; Intermittent failure on Kafka, see #1256
      (when-not (contains? #{:local-kafka :local-kafka-transit} *node-type*)
        (t/testing "tx fns return with-ops? correctly"
          (let [tx4 (fix/submit+await-tx [[::xt/put {:xt/id :jack :age 21}]
                                          [::xt/put {:xt/id :increment-age
                                                     ::xt/fn '(fn [ctx eid]
                                                                (let [db (xtdb.api/db ctx)
                                                                      entity (xtdb.api/entity db eid)]
                                                                  [[::xt/put (update entity :age inc)]]))}]
                                          [::xt/put {:xt/id :increment-age-2
                                                     ::xt/fn '(fn [ctx eid]
                                                                [[::xt/fn :increment-age eid]])}]
                                          [::xt/fn :increment-age-2 :jack]])]
            (t/is (true? (xt/tx-committed? *api* tx4)))
            (with-open [tx-log-iterator (xt/open-tx-log *api* nil true)]
              (let [tx-ops (-> tx-log-iterator iterator-seq last ::xt/tx-ops)]
                (t/is (= [::xt/fn
                          (c/new-id :increment-age-2)
                          {::xt/tx-ops [[::xt/fn
                                         (c/new-id :increment-age)
                                         {::xt/tx-ops [[::xt/put {:xt/id :jack, :age 22}]]}]]}]
                         (last tx-ops)))))))))))

(t/deftest test-history-api
  (letfn [(submit-ivan [m valid-time]
            (let [doc (merge {:xt/id :ivan, :name "Ivan"} m)]
              (merge (fix/submit+await-tx [[::xt/put doc valid-time]])
                     {::xt/doc doc
                      ::xt/valid-time valid-time
                      ::xt/content-hash (c/hash-doc doc)})))]
    (let [v1 (submit-ivan {:version 1} #inst "2019-02-01")
          v2 (submit-ivan {:version 2} #inst "2019-02-02")
          v3 (submit-ivan {:version 3} #inst "2019-02-03")
          v2-corrected (submit-ivan {:version 2, :corrected? true} #inst "2019-02-02")]

      (with-dbs [db (*api* #inst "2019-02-03")]
        (t/is (= [v1 v2-corrected v3]
                 (xt/entity-history db :ivan :asc {:with-docs? true})))

        (t/is (= [v3 v2-corrected v1]
                 (xt/entity-history db :ivan :desc {:with-docs? true})))

        (with-open [history-asc (xt/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (xt/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2-corrected v3]
                   (iterator-seq history-asc)))
          (t/is (= [v3 v2-corrected v1]
                   (iterator-seq history-desc)))))

      (with-dbs [db (*api* #inst "2019-02-02")]
        (t/is (= [v1 v2-corrected]
                 (xt/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2-corrected v1]
                 (xt/entity-history db :ivan :desc {:with-docs? true}))))

      (with-dbs [db (*api* #inst "2019-01-31")]
        (t/is (empty? (xt/entity-history db :ivan :asc)))
        (t/is (empty? (xt/entity-history db :ivan :desc)))

        (with-open [history-asc (xt/open-entity-history db :ivan :asc)
                    history-desc (xt/open-entity-history db :ivan :desc)]
          (t/is (empty? (iterator-seq history-asc)))
          (t/is (empty? (iterator-seq history-desc)))))

      (with-dbs [db (*api* #inst "2019-02-04")]
        (with-open [history-asc (xt/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (xt/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2-corrected v3]
                   (iterator-seq history-asc)))
          (t/is (= [v3 v2-corrected v1]
                   (iterator-seq history-desc)))))

      (with-dbs [db (*api* {::xt/valid-time #inst "2019-02-04", ::xt/tx-time #inst "2019-01-31"})]
        (t/is (empty? (xt/entity-history db :ivan :asc)))
        (t/is (empty? (xt/entity-history db :ivan :desc))))

      (with-dbs [db (*api* {::xt/valid-time #inst "2019-02-02", ::xt/tx v2})]
        (with-open [history-asc (xt/open-entity-history db :ivan :asc {:with-docs? true})
                    history-desc (xt/open-entity-history db :ivan :desc {:with-docs? true})]
          (t/is (= [v1 v2]
                   (iterator-seq history-asc)))
          (t/is (= [v2 v1]
                   (iterator-seq history-desc)))))

      (with-dbs [db (*api* {::xt/valid-time #inst "2019-02-03", ::xt/tx v2})]
        (t/is (= [v1 v2]
                 (xt/entity-history db :ivan :asc {:with-docs? true})))
        (t/is (= [v2 v1]
                 (xt/entity-history db :ivan :desc {:with-docs? true})))))))

(t/deftest test-db-throws-if-future-tx-time-provided-546
  (let [{::xt/keys [^Date tx-time]} (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])
        the-future (Date. (+ (.getTime tx-time) 10000))]
    (t/is (thrown? NodeOutOfSyncException (xt/db *api* the-future the-future)))))

(t/deftest test-db-is-a-snapshot
  (let [tx (fix/submit+await-tx [[::xt/put {:xt/id :foo, :count 0}]])
        db (xt/db *api*)]
    (t/is (= tx (::xt/tx (xt/db-basis db))))
    (t/is (= {:xt/id :foo, :count 0}
             (xt/entity db :foo)))

    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :count 1}]])

    (t/is (= {:xt/id :foo, :count 0}
             (xt/entity db :foo)))))

(t/deftest test-latest-submitted-tx
  (t/is (nil? (xt/latest-submitted-tx *api*)))
  (let [{::xt/keys [tx-id]} (xt/submit-tx *api* [[::xt/put {:xt/id :foo}]])]
    (t/is (= {::xt/tx-id tx-id}
             (xt/latest-submitted-tx *api*))))

  (xt/sync *api*)

  (t/is (= {:xt/id :foo} (xt/entity (xt/db *api*) :foo))))

(t/deftest test-listen-for-indexed-txs
  (when-not (contains? (set t/*testing-contexts*) (str :remote))
    (let [!events (atom [])]
      (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])

      (let [[bar-tx baz-tx] (with-open [_ (xt/listen *api* {::xt/event-type ::xt/indexed-tx
                                                            :with-tx-ops? true}
                                                     (fn [evt]
                                                       (swap! !events conj evt)))]

                              (let [bar-tx (fix/submit+await-tx [[::xt/put {:xt/id :bar}]])
                                    baz-tx (fix/submit+await-tx [[::xt/put {:xt/id :baz}]])]

                                (Thread/sleep 100)

                                [bar-tx baz-tx]))]

        (fix/submit+await-tx [[::xt/put {:xt/id :ivan}]])

        (Thread/sleep 100)

        (t/is (= [(merge {::xt/event-type ::xt/indexed-tx,
                          :committed? true
                          ::xt/tx-ops [[::xt/put {:xt/id :bar}]]}
                         bar-tx)
                  (merge {::xt/event-type ::xt/indexed-tx,
                          :committed? true
                          ::xt/tx-ops [[::xt/put {:xt/id :baz}]]}
                         baz-tx)]
                 @!events))))))

(t/deftest test-tx-fn-replacing-arg-docs-866
  (fix/submit+await-tx [[::xt/put {:xt/id :put-ivan
                                   ::xt/fn '(fn [ctx doc]
                                              [[::xt/put (assoc doc :xt/id :ivan)]])}]])

  (with-redefs [tx/tx-fn-eval-cache (memoize eval)]
    (t/testing "replaces args doc with resulting ops"
      (fix/submit+await-tx [[::xt/fn :put-ivan {:name "Ivan"}]])

      (t/is (= {:xt/id :ivan, :name "Ivan"}
               (xt/entity (xt/db *api*) :ivan)))

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
      (let [tx (xt/submit-tx *api* (for [n (range 100)] [::xt/put {:xt/id (str "test-" n)}]))]
        (t/is
         (thrown-with-msg?
          java.util.concurrent.TimeoutException
          #"Timed out waiting for: #:xt"
          (xt/await-tx *api* tx (Duration/ofNanos 1))))))))

(t/deftest missing-doc-halts-tx-ingestion
  (when-not (contains? #{:remote} *node-type*)
    (let [tx (db/submit-tx (:tx-log *api*) [[:crux.tx/put (c/new-id :foo) (c/new-id {:xt/id :foo})]])]
      (t/is (thrown-with-msg?
             IllegalStateException
             #"missing docs"
             (try
               (xt/await-tx *api* tx (Duration/ofMillis 5000))
               (catch Exception e
                 (throw (.getCause e)))))))))

(t/deftest round-trips-clobs
  ;; ensure that anyone changing this also checks this test
  (t/is (= 224 @#'c/max-value-index-length))

  (let [clob (pr-str (range 1000))
        clob-doc {:xt/id :clob, :clob clob}]
    (fix/submit+await-tx [[::xt/put clob-doc]])

    (let [db (xt/db *api*)]
      (t/is (= #{[clob]}
               (xt/q db '{:find [?clob]
                          :where [[?e :clob ?clob]]})))

      (t/is (= #{[clob-doc]}
               (xt/q db '{:find [(pull ?e [*])]
                          :where [[?e :clob]]})))

      (t/is (= clob-doc
               (xt/entity db :clob))))))
