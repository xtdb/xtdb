(ns xtdb.tx-fn-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.node :as node]
            [xtdb.indexer :as idx]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-call-tx-fn
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [[:put-fn :my-fn
                              '(fn [id n]
                                 [[:put :foo {:xt/id id, :n n}]])]
                             [:call :my-fn :foo 0]
                             [:call :my-fn :bar 1]])

    (t/is (= [{:xt/id :foo, :n 0}
              {:xt/id :bar, :n 1}]
             (xt/q tu/*node*
                   '{:find [xt/id n]
                     :where [(match :foo [xt/id n])]}))))

  (t/testing "nested tx fn"
    (xt/submit-tx tu/*node* [[:put-fn :inner-fn
                              '(fn [id]
                                 [[:put :bar {:xt/id (keyword (str (name id) "-inner")), :from :inner}]])]
                             [:put-fn :outer-fn
                              '(fn [id]
                                 [[:call :inner-fn id]
                                  [:put :bar {:xt/id (keyword (str (name id) "-outer")), :from :outer}]])]
                             [:call :inner-fn :foo]
                             [:call :outer-fn :bar]])

    (t/is (= [{:xt/id :foo-inner, :from :inner}
              {:xt/id :bar-inner, :from :inner}
              {:xt/id :bar-outer, :from :outer}]
             (xt/q tu/*node*
                   '{:find [xt/id from]
                     :where [(match :bar [xt/id from])]})))))

(t/deftest test-tx-fn-return-values
  (xt/submit-tx tu/*node* [[:put-fn :identity 'identity]])

  (letfn [(run-test [ret-val put-id]
            (xt/submit-tx tu/*node* [[:call :identity ret-val]
                                     [:put :docs {:xt/id put-id}]])

            (->> (xt/q tu/*node*
                       '{:find [id]
                         :in [id]
                         :where [($ :docs {:xt/id id})]}
                       put-id)
                 (into #{} (map :id))))]

    (t/is (= #{:empty-list} (run-test [] :empty-list)))
    (t/is (= #{} (run-test false :false)))
    (t/is (= #{:nil} (run-test nil :nil)))
    (t/is (= #{:true} (run-test true :true)))))

(t/deftest test-tx-fn-q
  (xt/submit-tx tu/*node* [[:put-fn :doc-counter
                            '(fn [id]
                               (let [doc-count (count (q '{:find [id]
                                                           :where [(match :foo [id])]}))]
                                 [[:put :foo {:xt/id id, :doc-count doc-count}]]))]
                           [:call :doc-counter :foo]
                           [:call :doc-counter :bar]])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '{:find [xt/id doc-count]
                   :where [(match :foo [xt/id doc-count])]})))

  (let [tx2 (xt/submit-tx tu/*node* [[:put :accounts {:xt/id :petr :balance 100}]
                                     [:put :accounts {:xt/id :ivan :balance 200}]
                                     [:put-fn :update-balance
                                      '(fn [id]
                                         (let [[account] (q '{:find [xt/id balance]
                                                              :in [xt/id]
                                                              :where [(match :accounts [xt/id balance])]}
                                                            id)]
                                           (if account
                                             [[:put :accounts (update account :balance inc)]]
                                             [])))]
                                     [:call :update-balance :petr]
                                     [:call :update-balance :undefined]])]
    (t/is (= #{{:xt/id :petr, :balance 101}
               {:xt/id :ivan, :balance 200}}
             (set (xt/q tu/*node*
                        (-> '{:find [xt/id balance]
                              :where [(match :accounts [xt/id balance])]}
                            (assoc :basis {:tx tx2})))))
          "query in tx-fn with in-args")))

(t/deftest test-tx-fn-sql-q
  (xt/submit-tx tu/*node* [[:put-fn :doc-counter
                            '(fn [id]
                               (let [[{doc-count :doc_count}] (sql-q "SELECT COUNT(*) doc_count FROM docs")]
                                 [[:put :docs {:xt/id id, :doc-count doc-count}]]))]
                           [:call :doc-counter :foo]
                           [:call :doc-counter :bar]])

  (t/is (= [{:xt/id :foo, :doc-count 0}
            {:xt/id :bar, :doc-count 1}]
           (xt/q tu/*node*
                 '{:find [xt/id doc-count]
                   :where [(match :docs [xt/id doc-count])]}))))

(t/deftest test-tx-fn-current-tx
  (let [{tt0 :system-time} (xt/submit-tx tu/*node* [[:put-fn :with-tx
                                                  '(fn [id]
                                                     [[:put :docs (into {:xt/id id} *current-tx*)]])]
                                                 [:call :with-tx :foo]
                                                 [:call :with-tx :bar]])

        {tt1 :system-time} (xt/submit-tx tu/*node* [[:call :with-tx :baz]])]

    (t/is (= [{:xt/id :foo, :tx-id 0, :system-time (util/->zdt tt0)}
              {:xt/id :bar, :tx-id 0, :system-time (util/->zdt tt0)}
              {:xt/id :baz, :tx-id 1, :system-time (util/->zdt tt1)}]
             (xt/q tu/*node*
                   '{:find [xt/id tx-id system-time]
                     :where [(match :docs [xt/id tx-id system-time])]})))))

(t/deftest test-tx-fn-exceptions
  (letfn [(foo-version []
            (-> (xt/q tu/*node*
                      '{:find [xt/id version], :where [(match :docs [xt/id version])]})
                first :version))]

    (xt/submit-tx tu/*node* [[:put-fn :assoc-version
                              '(fn [version]
                                [[:put :docs {:xt/id :foo, :version version}]])]
                             [:call :assoc-version 0]])
    (t/is (= 0 (foo-version)))

    (t/testing "non existing tx fn"
      (xt/submit-tx tu/*node* '[[:call :non-existing-fn]
                                [:call :assoc-version :fail]])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException
                              #":xtdb.call/no-such-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "invalid results"
      (xt/submit-tx tu/*node* [[:put-fn :invalid-fn '(fn [] [[:foo]])]])
      (xt/submit-tx tu/*node* '[[:call :invalid-fn]
                                [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? IllegalArgumentException #"Invalid tx op"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "no :fn"
      (xt/submit-tx tu/*node* [[:put :xt/tx-fns {:xt/id :no-fn}]])

      (xt/submit-tx tu/*node* '[[:call :no-fn]
                                [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #":xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "not a fn"
      (xt/submit-tx tu/*node* [[:put :xt/tx-fns {:xt/id :not-a-fn, :xt/fn 0}]])
      (xt/submit-tx tu/*node* '[[:call :not-a-fn]
                                [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))

      (t/is (thrown-with-msg? RuntimeException
                              #":xtdb.call/invalid-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "compilation errors"
      (xt/submit-tx tu/*node* [[:put-fn :compilation-error-fn '(fn [] unknown-symbol)]])
      (xt/submit-tx tu/*node* '[[:call :compilation-error-fn]
                                [:call :assoc-version :fail]])

      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException #":xtdb.call/error-compiling-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "exception thrown"
      #_{:clj-kondo/ignore [:unused-value]}
      (xt/submit-tx tu/*node* [[:put-fn :exception-fn
                                '(fn []
                                   (/ 1 0)
                                   [])]])

      (xt/submit-tx tu/*node* '[[:call :exception-fn]
                                [:call :assoc-version :fail]])
      (t/is (= 0 (foo-version)))
      (t/is (thrown-with-msg? RuntimeException #":xtdb.call/error-evaluating-tx-fn"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "still working after all these errors"
      (xt/submit-tx tu/*node* [[:call :update-version :done]])
      (= :done (foo-version)))))

(t/deftest handle-interrupted-exception-614
  (t/is (thrown-with-msg?
         Exception #"sleep interrupted"
         @(with-open [node (node/start-node {})]
            (xt/submit-tx node [[:put-fn :hello-world
                                 '(fn hello-world [id]
                                    (sleep 200)
                                    [[:put :xt_docs {:xt/id id :foo (str id)}]])]])
            (xt/submit-tx node [[:call :hello-world 1]])

            (Thread/sleep 100)

            (xt/q& node '{:find [xt/id]
                          :where [(match :xt_docs [xt/id])
                                  [id :foo]]})))))


(t/deftest test-call-tx-fn-with-ns-attr
  (t/testing "simple call"
    (xt/submit-tx tu/*node* [[:put-fn :my-fn
                              '(fn [id n]
                                 [[:put :docs {:xt/id id, :a/b n}]])]
                             [:call :my-fn :foo 0]
                             [:call :my-fn :bar 1]])

    (t/is (= [{:xt/id :foo, :n 0}
              {:xt/id :bar, :n 1}]
             (xt/q tu/*node*
                   '{:find [xt/id n]
                     :where [(match :docs [xt/id {:a/b n}])]})))))
