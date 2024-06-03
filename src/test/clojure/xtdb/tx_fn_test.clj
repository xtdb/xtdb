(ns xtdb.tx-fn-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.serde :as serde]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-call-tx-fn
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [[:put-fn :my-fn
                              '(fn [id n]
                                 [[:put-docs :foo {:xt/id id, :n n}]])]
                             [:call :my-fn :foo 0]
                             [:call :my-fn :bar 1]])

    (t/is (= #{{:xt/id :foo, :n 0}
               {:xt/id :bar, :n 1}}
             (set (xt/q tu/*node*
                        '(from :foo [xt/id n]))))))

  (t/testing "nested tx fn"
    (xt/submit-tx tu/*node* [[:put-fn :inner-fn
                              '(fn [id]
                                 [[:put-docs :bar {:xt/id (keyword (str (name id) "-inner")), :from :inner}]])]
                             [:put-fn :outer-fn
                              '(fn [id]
                                 [[:call :inner-fn id]
                                  [:put-docs :bar {:xt/id (keyword (str (name id) "-outer")), :from :outer}]])]
                             [:call :inner-fn :foo]
                             [:call :outer-fn :bar]])

    (t/is (= #{{:xt/id :foo-inner, :from :inner}
               {:xt/id :bar-inner, :from :inner}
               {:xt/id :bar-outer, :from :outer}}
             (set (xt/q tu/*node*
                        '(from :bar [xt/id from])))))))

(t/deftest test-tx-fn-return-values
  (xt/submit-tx tu/*node* [[:put-fn :identity 'identity]])

  (letfn [(run-test [ret-val put-id]
            (xt/submit-tx tu/*node* [[:call :identity ret-val]
                                     [:put-docs :docs {:xt/id put-id}]])

            (->> (xt/q tu/*node*
                       '(from :docs [{:xt/id id} {:xt/id $id}])
                       {:args {:id put-id}})
                 (into #{} (map :id))))]

    (t/is (= #{:empty-list} (run-test [] :empty-list)))
    (t/is (= #{} (run-test false :false)))
    (t/is (= #{:nil} (run-test nil :nil)))
    (t/is (= #{:true} (run-test true :true)))))

(t/deftest test-tx-fn-q
  (xt/submit-tx tu/*node* [[:put-fn :doc-counter
                            '(fn [id]
                               (let [doc-count (count (q '(from :foo [xt/id])))]
                                 [[:put-docs :foo {:xt/id id, :doc-count doc-count}]]))]
                           [:call :doc-counter :foo]
                           [:call :doc-counter :bar]])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '(from :foo [xt/id doc-count]))))

  (let [tx2 (xt/submit-tx tu/*node* [[:put-docs :accounts {:xt/id :petr :balance 100}]
                                     [:put-docs :accounts {:xt/id :ivan :balance 200}]
                                     [:put-fn :update-balance
                                      '(fn [id]
                                         (let [[account] (q '(from :accounts [balance xt/id {:xt/id $id}])
                                                            {:args {:id id}})]
                                           (if account
                                             [[:put-docs :accounts (update account :balance inc)]]
                                             [])))]
                                     [:call :update-balance :petr]
                                     [:call :update-balance :undefined]])]
    (t/is (= #{{:xt/id :petr, :balance 101}
               {:xt/id :ivan, :balance 200}}
             (set (xt/q tu/*node*
                        '(from :accounts [xt/id balance])
                        {:basis {:at-tx tx2}})))
          "query in tx-fn with in-args")))

(t/deftest test-tx-fn-sql-q
  (xt/submit-tx tu/*node* [[:put-fn :doc-counter
                            '(fn [id]
                               (let [[{:keys [doc-count]}] (q "SELECT COUNT(*) doc_count FROM docs")]
                                 [[:put-docs :docs {:xt/id id, :doc-count doc-count}]]))]
                           [:call :doc-counter :foo]
                           [:call :doc-counter :bar]])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '(from :docs [xt/id doc-count])))))

(t/deftest test-tx-fn-current-tx
  (let [tx0 (xt/submit-tx tu/*node* [[:put-fn :with-tx
                                      '(fn [id]
                                         [[:put-docs :docs (into {:xt/id id} *current-tx*)]])]
                                     [:call :with-tx :foo]
                                     [:call :with-tx :bar]])
        tt0 (.getSystemTime tx0)
        tx1 (xt/submit-tx tu/*node* [[:call :with-tx :baz]])
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

    (xt/submit-tx tu/*node* [[:put-fn :assoc-version
                              '(fn [version]
                                 [[:put-docs :docs {:xt/id :foo, :version version}]])]
                             [:call :assoc-version 0]])
    (t/is (= 0 (foo-version)))

    (t/testing "non existing tx fn"
      (xt/submit-tx tu/*node* [[:call :non-existing-fn]
                               [:call :assoc-version :fail]])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/no-such-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "invalid results"
      (xt/submit-tx tu/*node* [[:put-fn :invalid-fn '(fn [] [[:foo]])]])
      (xt/submit-tx tu/*node* [[:call :invalid-fn]
                               [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? IllegalArgumentException #"unknown-tx-op"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "no :fn"
      (xt/submit-tx tu/*node* [[:put-docs :xt/tx-fns {:xt/id :no-fn}]])

      (xt/submit-tx tu/*node* [[:call :no-fn]
                               [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "not a fn"
      (xt/submit-tx tu/*node* [[:put-docs :xt/tx-fns {:xt/id :not-a-fn, :xt/fn 0}]])
      (xt/submit-tx tu/*node* [[:call :not-a-fn]
                               [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #"xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "compilation errors"
      (xt/submit-tx tu/*node* [[:put-fn :compilation-error-fn '(fn [] unknown-symbol)]])
      (xt/submit-tx tu/*node* [[:call :compilation-error-fn]
                               [:call :assoc-version :fail]])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException #"xtdb.call/error-compiling-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "exception thrown"
      #_{:clj-kondo/ignore [:unused-value]}
      (xt/submit-tx tu/*node* [[:put-fn :exception-fn
                                '(fn []
                                   (/ 1 0)
                                   [])]])

      (tu/with-log-level 'xtdb.indexer :error
        (xt/submit-tx tu/*node* [[:call :exception-fn]
                                 [:call :assoc-version :fail]])
        (t/is (= 0 (foo-version)))
        (t/is (thrown-with-msg? RuntimeException #"xtdb.call/error-evaluating-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "still working after all these errors"
      (xt/submit-tx tu/*node* [[:call :update-version :done]])
      (= :done (foo-version)))))

(t/deftest handle-interrupted-exception-614
  (t/is (thrown-with-msg?
         Exception #"sleep interrupted"
         @(with-open [node (xtn/start-node {})]
            (xt/submit-tx node [[:put-fn :hello-world
                                 '(fn hello-world [id]
                                    (sleep 200)
                                    [[:put-docs :xt_docs {:xt/id id :foo (str id)}]])]])
            (xt/submit-tx node [[:call :hello-world 1]])

            (future
              (xt/q node '(from :xt_docs [xt/id])))))))

(t/deftest test-call-tx-fn-with-ns-attr
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [[:put-fn :my-fn
                              '(fn [id n]
                                 [[:put-docs :docs {:xt/id id, :a/b n}]])]
                             [:call :my-fn :foo 0]
                             [:call :my-fn :bar 1]])

    (t/is (= [{:xt/id :foo, :n 0}
              {:xt/id :bar, :n 1}]
             (xt/q tu/*node* '(from :docs [xt/id {:a/b n}]))))))

(t/deftest test-lazy-error-in-tx-fns-2811
  (xt/submit-tx tu/*node* [[:put-fn :my-fn '(fn [ns] (for [n ns]
                                                       (if (< n 100)
                                                         [:put-docs :foo {:xt/id n :v n}]
                                                         (throw (ex-info "boom" {})))))]])
  (xt/submit-tx tu/*node* [[:call :my-fn (range 200)]])
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :v 1}]])
  (t/is (= [{:xt/id 1, :v 1}]
           (xt/q tu/*node*
                 '(from :docs [xt/id v])))))

(t/deftest normalisation-in-tx-fn
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :first-name "Allan" :last-name "Turing"}]
                           [:put-fn :my-fn '(fn []
                                              (let [ks (->> (q '(from :docs [first-name last-name])
                                                               {:key-fn :snake-case-keyword})
                                                            (mapcat keys)
                                                            (into []))]
                                                [[:put-docs :the-keys {:xt/id 1 :keys ks}]]))]
                           [:call :my-fn]])

  (t/is (= #{{:key :first_name} {:key :last_name}}
           (set (xt/q tu/*node*
                      '(-> (from :the-keys [keys])
                           (unnest {:key keys})
                           (return key)))))
        "testing `key-fn` in q")

  (xt/submit-tx tu/*node* [[:put-fn :my-case-fn '(fn [{:keys [snake_case kebab-case]}]
                                                   (cond-> []
                                                     snake_case (conj [:put-docs :casing {:xt/id snake_case}])
                                                     kebab-case (conj [:put-docs :casing {:xt/id kebab-case}])))]
                           [:call :my-case-fn {:snake_case "foo"}]
                           [:call :my-case-fn {:kebab-case "bar"}]
                           [:call :my-case-fn {:snake_case "baz" :kebab-case "toto"}]])

  (t/is (= [{:xt/id "baz"} {:xt/id "toto"} {:xt/id "bar"} {:xt/id "foo"}]
           (xt/q tu/*node*
                 '(from :casing [xt/id])))))

(t/deftest test-sql-error
  (let [res (xt/execute-tx tu/*node* [[:put-fn :my-fn '(fn [] (q "SELECT * FORM foo"))]
                                      [:call :my-fn]])]
    (t/is (= (serde/->tx-aborted 0 (time/->instant #inst "2020") nil)
             (assoc res :error nil)))

    (t/is (re-find #"line 1:9 mismatched input 'FORM'" (ex-message (:error res))))))
