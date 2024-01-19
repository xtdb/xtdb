(ns xtdb.tx-fn-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-call-tx-fn
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [(xt/put-fn :my-fn
                                        '(fn [id n]
                                           [(xt/put :foo {:xt/id id, :n n})]))
                             (xt/call :my-fn :foo 0)
                             (xt/call :my-fn :bar 1)])

    (t/is (= #{{:xt/id :foo, :n 0}
               {:xt/id :bar, :n 1}}
             (set (xt/q tu/*node*
                        '(from :foo [xt/id n]))))))

  (t/testing "nested tx fn"
    (xt/submit-tx tu/*node* [(xt/put-fn :inner-fn
                                        '(fn [id]
                                           [(xt/put :bar {:xt/id (keyword (str (name id) "-inner")), :from :inner})]))
                             (xt/put-fn :outer-fn
                                        '(fn [id]
                                           [(xt/call :inner-fn id)
                                            (xt/put :bar {:xt/id (keyword (str (name id) "-outer")), :from :outer})]))
                             (xt/call :inner-fn :foo)
                             (xt/call :outer-fn :bar)])

    (t/is (= #{{:xt/id :foo-inner, :from :inner}
               {:xt/id :bar-inner, :from :inner}
               {:xt/id :bar-outer, :from :outer}}
             (set (xt/q tu/*node*
                        '(from :bar [xt/id from])))))))

(t/deftest test-tx-fn-return-values
  (xt/submit-tx tu/*node* [(xt/put-fn :identity 'identity)])

  (letfn [(run-test [ret-val put-id]
            (xt/submit-tx tu/*node* [(xt/call :identity ret-val)
                                     (xt/put :docs {:xt/id put-id})])

            (->> (xt/q tu/*node*
                       '(from :docs [{:xt/id id} {:xt/id $id}])
                       {:args {:id put-id}})
                 (into #{} (map :id))))]

    (t/is (= #{:empty-list} (run-test [] :empty-list)))
    (t/is (= #{} (run-test false :false)))
    (t/is (= #{:nil} (run-test nil :nil)))
    (t/is (= #{:true} (run-test true :true)))))

(t/deftest test-tx-fn-q
  (xt/submit-tx tu/*node* [(xt/put-fn :doc-counter
                                      '(fn [id]
                                         (let [doc-count (count (q '(from :foo [xt/id])))]
                                           [(xt/put :foo {:xt/id id, :doc-count doc-count})])))
                           (xt/call :doc-counter :foo)
                           (xt/call :doc-counter :bar)])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '(from :foo [xt/id doc-count]))))

  (let [tx2 (xt/submit-tx tu/*node* [(xt/put :accounts {:xt/id :petr :balance 100})
                                     (xt/put :accounts {:xt/id :ivan :balance 200})
                                     (xt/put-fn :update-balance
                                                '(fn [id]
                                                   (let [[account] (q '(from :accounts [balance xt/id {:xt/id $id}])
                                                                      {:args {:id id}})]
                                                     (if account
                                                       [(xt/put :accounts (update account :balance inc))]
                                                       []))))
                                     (xt/call :update-balance :petr)
                                     (xt/call :update-balance :undefined)])]
    (t/is (= #{{:xt/id :petr, :balance 101}
               {:xt/id :ivan, :balance 200}}
             (set (xt/q tu/*node*
                        '(from :accounts [xt/id balance])
                        {:basis {:at-tx tx2}})))
          "query in tx-fn with in-args")))

(t/deftest test-tx-fn-sql-q
  (xt/submit-tx tu/*node* [(xt/put-fn :doc-counter
                                      '(fn [id]
                                         (let [[{:keys [doc-count]}] (q "SELECT COUNT(*) doc_count FROM docs")]
                                           [(xt/put :docs {:xt/id id, :doc-count doc-count})])))
                           (xt/call :doc-counter :foo)
                           (xt/call :doc-counter :bar)])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '(from :docs [xt/id doc-count])))))

(t/deftest test-tx-fn-current-tx
  (let [tx0 (xt/submit-tx tu/*node* [(xt/put-fn :with-tx
                                                '(fn [id]
                                                   [(xt/put :docs (into {:xt/id id} *current-tx*))]))
                                     (xt/call :with-tx :foo)
                                     (xt/call :with-tx :bar)])
        tt0 (.getSystemTime tx0)
        tx1 (xt/submit-tx tu/*node* [(xt/call :with-tx :baz)])
        tt1 (.getSystemTime tx1)]

    (t/is (= #{{:xt/id :foo, :tx-id 0, :system-time (time/->zdt tt0)}
               {:xt/id :bar, :tx-id 0, :system-time (time/->zdt tt0)}
               {:xt/id :baz, :tx-id 1, :system-time (time/->zdt tt1)}}
             (set (xt/q tu/*node*
                        '(from :docs [xt/id tx-id system-time])))))))

(t/deftest test-tx-fn-exceptions
  (letfn [(foo-version []
            (-> (xt/q tu/*node*
                      '(from :docs [xt/id version]))
                first :version))]

    (xt/submit-tx tu/*node* [(xt/put-fn :assoc-version
                                        '(fn [version]
                                           [(xt/put :docs {:xt/id :foo, :version version})]))
                             (xt/call :assoc-version 0)])
    (t/is (= 0 (foo-version)))

    (t/testing "non existing tx fn"
      (xt/submit-tx tu/*node* [(xt/call :non-existing-fn)
                               (xt/call :assoc-version :fail)])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/no-such-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "invalid results"
      (xt/submit-tx tu/*node* [(xt/put-fn :invalid-fn '(fn [] [[:foo]]))])
      (xt/submit-tx tu/*node* [(xt/call :invalid-fn)
                               (xt/call :assoc-version :fail)])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? IllegalArgumentException #"invalid-tx-op"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "no :fn"
      (xt/submit-tx tu/*node* [(xt/put :xt/tx-fns {:xt/id :no-fn})])

      (xt/submit-tx tu/*node* [(xt/call :no-fn)
                               (xt/call :assoc-version :fail)])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "not a fn"
      (xt/submit-tx tu/*node* [(xt/put :xt/tx-fns {:xt/id :not-a-fn, :xt/fn 0})])
      (xt/submit-tx tu/*node* [(xt/call :not-a-fn)
                               (xt/call :assoc-version :fail)])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "compilation errors"
      (xt/submit-tx tu/*node* [(xt/put-fn :compilation-error-fn '(fn [] unknown-symbol))])
      (xt/submit-tx tu/*node* [(xt/call :compilation-error-fn)
                               (xt/call :assoc-version :fail)])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException #"xtdb.call/error-compiling-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "exception thrown"
      #_{:clj-kondo/ignore [:unused-value]}
      (xt/submit-tx tu/*node* [(xt/put-fn :exception-fn
                                          '(fn []
                                             (/ 1 0)
                                             []))])

      (tu/with-log-level 'xtdb.indexer :error
        (xt/submit-tx tu/*node* [(xt/call :exception-fn)
                                 (xt/call :assoc-version :fail)])
        (t/is (= 0 (foo-version)))
        (t/is (thrown-with-msg? RuntimeException #"xtdb.call/error-evaluating-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "still working after all these errors"
      (xt/submit-tx tu/*node* [(xt/call :update-version :done)])
      (= :done (foo-version)))))

(t/deftest handle-interrupted-exception-614
  (t/is (thrown-with-msg?
         Exception #"sleep interrupted"
         @(with-open [node (xtn/start-node {})]
            (xt/submit-tx node [(xt/put-fn :hello-world
                                           '(fn hello-world [id]
                                              (sleep 200)
                                              [(xt/put :xt_docs {:xt/id id :foo (str id)})]))])
            (xt/submit-tx node [(xt/call :hello-world 1)])

            (Thread/sleep 100)

            (xt/q& node '(from :xt_docs [xt/id]))))))


(t/deftest test-call-tx-fn-with-ns-attr
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [(xt/put-fn :my-fn
                                        '(fn [id n]
                                           [(xt/put :docs {:xt/id id, :a/b n})]))
                             (xt/call :my-fn :foo 0)
                             (xt/call :my-fn :bar 1)])

    (t/is (= [{:xt/id :foo, :n 0}
              {:xt/id :bar, :n 1}]
             (xt/q tu/*node* '(from :docs [xt/id {:a/b n}]))))))

(t/deftest test-lazy-error-in-tx-fns-2811
  (xt/submit-tx tu/*node* [(xt/put-fn :my-fn '(fn [ns] (for [n ns]
                                                         (if (< n 100)
                                                           (xt/put :foo {:xt/id n :v n})
                                                           (throw (ex-info "boom" {}))))))])
  (xt/submit-tx tu/*node* [(xt/call :my-fn (range 200))])
  (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id 1 :v 1})])
  (t/is (= [{:xt/id 1, :v 1}]
           (xt/q tu/*node*
                 '(from :docs [xt/id v])))))

(t/deftest normalisation-in-tx-fn
  (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id 1 :first-name "Allan" :last-name "Turing"})
                           (xt/put-fn :my-fn '(fn []
                                                (let [ks (->> (q '(from :docs [first-name last-name])
                                                                 {:key-fn :snake-case-keyword})
                                                              (mapcat keys)
                                                              (into []))]
                                                  [(xt/put :the-keys {:xt/id 1 :keys ks})])))
                           (xt/call :my-fn)])

  (t/is (= #{{:key :first_name} {:key :last_name}}
           (set (xt/q tu/*node*
                      '(-> (from :the-keys [keys])
                           (unnest {:key keys})
                           (return key)))))
        "testing `key-fn` in q")

  (xt/submit-tx tu/*node* [(xt/put-fn :my-case-fn '(fn [{:keys [snake_case kebab-case]}]
                                                     (cond-> []
                                                       snake_case (conj (xt/put :casing {:xt/id snake_case}))
                                                       kebab-case (conj (xt/put :casing {:xt/id kebab-case})))))
                           (xt/call :my-case-fn {:snake_case "foo"})
                           (xt/call :my-case-fn {:kebab-case "bar"})
                           (xt/call :my-case-fn {:snake_case "baz" :kebab-case "toto"})])

  (t/is (= [{:xt/id "baz"} {:xt/id "toto"} {:xt/id "bar"} {:xt/id "foo"}]
           (xt/q tu/*node*
                 '(from :casing [xt/id])))))

(t/deftest test-unhandled-query-type
  (xt/submit-tx tu/*node* [(xt/put-fn :my-fn '(fn [] (q "SELECT t1.foo FROM foo")))
                           (xt/call :my-fn)])

  (t/is (= "Runtime error: 'xtdb.call/error-evaluating-tx-fn'"
           (-> (xt/q tu/*node* '(from :xt/txs [{:xt/error error}]))
               first
               :error
               ex-message))))
