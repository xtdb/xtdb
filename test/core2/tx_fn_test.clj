(ns core2.tx-fn-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.node :as node]
            [core2.indexer :as idx]
            [core2.test-util :as tu]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-call-tx-fn
  (t/testing "simple call"
    (let [!tx (c2/submit-tx tu/*node* [[:put {:id :my-fn,
                                              :fn #c2/clj-form (fn [id n]
                                                                 [[:put {:id id, :n n}]])}]
                                       [:call :my-fn :foo 0]
                                       [:call :my-fn :bar 1]])]
      (t/is (= [{:id :foo, :n 0}
                {:id :bar, :n 1}]
               (c2/datalog-query tu/*node*
                                 (-> '{:find [id n]
                                       :where [[id :n n]]}
                                     (assoc :basis {:tx !tx})))))))

  (t/testing "nested tx fn"
    (let [!tx (c2/submit-tx tu/*node* [[:put {:id :inner-fn,
                                              :fn #c2/clj-form (fn [id]
                                                                 [[:put {:id (keyword (str (name id) "-inner")), :from :inner}]])}]
                                       [:put {:id :outer-fn,
                                              :fn #c2/clj-form (fn [id]
                                                                 [[:call :inner-fn id]
                                                                  [:put {:id (keyword (str (name id) "-outer")), :from :outer}]])}]
                                       [:call :inner-fn :foo]
                                       [:call :outer-fn :bar]])]
      (t/is (= [{:id :foo-inner, :from :inner}
                {:id :bar-inner, :from :inner}
                {:id :bar-outer, :from :outer}]
               (c2/datalog-query tu/*node*
                                 (-> '{:find [id from]
                                       :where [[id :from from]]}
                                     (assoc :basis {:tx !tx}))))))))

(t/deftest test-tx-fn-return-values
  (c2/submit-tx tu/*node* [[:put {:id :identity,
                                  :fn #c2/clj-form identity}]])

  (letfn [(run-test [ret-val put-id]
            (let [!tx (c2/submit-tx tu/*node* [[:call :identity ret-val]
                                               [:put {:id put-id}]])]
              (->> (c2/datalog-query tu/*node*
                                     (-> '{:find [id]
                                           :in [id]
                                           :where [[id :id]]}
                                         (assoc :basis {:tx !tx}))
                                     put-id)
                   (into #{} (map :id)))))]

    (t/is (= #{:empty-list} (run-test [] :empty-list)))
    (t/is (= #{} (run-test false :false)))
    (t/is (= #{:nil} (run-test nil :nil)))
    (t/is (= #{:true} (run-test true :true)))))

(t/deftest test-tx-fn-q
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :doc-counter,
                                            :fn #c2/clj-form (fn [id]
                                                               (let [doc-count (count (q '{:find [id]
                                                                                           :where [[id :id]]}))]
                                                                 [[:put {:id id, :doc-count doc-count}]]))}]
                                     [:call :doc-counter :foo]
                                     [:call :doc-counter :bar]])]
    (t/is (= [{:id :foo, :doc-count 1}
              {:id :bar, :doc-count 2}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [id doc-count]
                                     :where [[id :doc-count doc-count]]}
                                   (assoc :basis {:tx !tx}))))))

  (let [!tx2 (c2/submit-tx tu/*node* [[:put {:id :petr :balance 100}]
                                      [:put {:id :ivan :balance 200}]
                                      [:put {:id :update-balance,
                                             :fn #c2/clj-form (fn [id]
                                                                (let [[account] (q '{:find [id balance]
                                                                                     :in [id]
                                                                                     :where [[id :balance balance]]}
                                                                                   id)]
                                                                  (if account
                                                                    [[:put (update account :balance inc)]]
                                                                    [])))}]
                                      [:call :update-balance :petr]
                                      [:call :update-balance :undefined]])]
    (t/is (= #{{:id :petr, :balance 101}
               {:id :ivan, :balance 200}}
             (set (c2/datalog-query tu/*node*
                                    (-> '{:find [id balance]
                                          :where [[id :balance balance]]}
                                        (assoc :basis {:tx !tx2})))))
          "query in tx-fn with in-args")))

(t/deftest test-tx-fn-sql-q
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :doc-counter,
                                            :fn #c2/clj-form (fn [id]
                                                               (let [[{doc-count :doc_count}] (sql-q "SELECT COUNT(*) doc_count FROM xt_docs")]
                                                                 [[:put {:id id, :doc-count doc-count}]]))}]
                                     [:call :doc-counter :foo]
                                     [:call :doc-counter :bar]])]
    (t/is (= [{:id :foo, :doc-count 1}
              {:id :bar, :doc-count 2}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [id doc-count]
                                     :where [[id :doc-count doc-count]]}
                                   (assoc :basis {:tx !tx})))))))

(t/deftest test-tx-fn-current-tx
  (let [{tt0 :sys-time} @(c2/submit-tx tu/*node* [[:put {:id :with-tx
                                                         :fn #c2/clj-form (fn [id]
                                                                            [[:put (into {:id id} *current-tx*)]])}]
                                                  [:call :with-tx :foo]
                                                  [:call :with-tx :bar]])

        {tt1 :sys-time, :as tx1} @(c2/submit-tx tu/*node* [[:call :with-tx :baz]])]

    (t/is (= [{:id :foo, :tx-id 0, :sys-time (util/->zdt tt0)}
              {:id :bar, :tx-id 0, :sys-time (util/->zdt tt0)}
              {:id :baz, :tx-id 1, :sys-time (util/->zdt tt1)}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [id tx-id sys-time]
                                     :where [[id :tx-id tx-id]
                                             [id :sys-time sys-time]]}
                                   (assoc :basis {:tx tx1})))))))

(t/deftest test-tx-fn-exceptions
  (letfn [(foo-version [!tx]
            (-> (c2/datalog-query tu/*node*
                                  (-> '{:find [id version], :where [[id :version version]]}
                                      (assoc :basis {:tx !tx})))
                first :version))]

    (let [!tx (c2/submit-tx tu/*node* '[[:put {:id :assoc-version
                                               :fn #c2/clj-form (fn [version]
                                                                  [[:put {:id :foo, :version version}]])}]
                                        [:call :assoc-version 0]])]
      (t/is (= 0 (foo-version !tx))))

    (t/testing "non existing tx fn"
      (let [!tx (c2/submit-tx tu/*node* '[[:call :non-existing-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx)))
        (t/is (thrown-with-msg? RuntimeException
                                #":core2.call/no-such-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "invalid results"
      (c2/submit-tx tu/*node* [[:put {:id :invalid-fn
                                      :fn #c2/clj-form (fn []
                                                         [[:foo]])}]])
      (let [!tx (c2/submit-tx tu/*node* '[[:call :invalid-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx))))

      (t/is (thrown-with-msg? IllegalArgumentException #"Invalid tx op"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "no :fn"
      (c2/submit-tx tu/*node* [[:put {:id :no-fn}]])

      (let [!tx (c2/submit-tx tu/*node* '[[:call :no-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx)))
        (t/is (thrown-with-msg? RuntimeException
                                #":core2.call/no-such-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "not a fn"
      (c2/submit-tx tu/*node* [[:put {:id :not-a-fn, :fn 0}]])
      (let [!tx (c2/submit-tx tu/*node* '[[:call :not-a-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx)))

        (t/is (thrown-with-msg? RuntimeException
                                #":core2.call/invalid-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "compilation errors"
      (c2/submit-tx tu/*node* [[:put {:id :compilation-error-fn
                                      :fn #c2/clj-form (fn [] unknown-symbol)}]])
      (let [!tx (c2/submit-tx tu/*node* '[[:call :compilation-error-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx)))
        (t/is (thrown-with-msg? RuntimeException #":core2.call/error-compiling-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "exception thrown"
      #_{:clj-kondo/ignore [:unused-value]}
      (c2/submit-tx tu/*node* [[:put {:id :exception-fn
                                      :fn #c2/clj-form (fn []
                                                         (/ 1 0)
                                                         [])}]])

      (let [!tx (c2/submit-tx tu/*node* '[[:call :exception-fn]
                                          [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version !tx)))
        (t/is (thrown-with-msg? RuntimeException #":core2.call/error-evaluating-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "still working after all these errors"
      (let [!tx (c2/submit-tx tu/*node* [[:call :update-version :done]])]
        (= :done (foo-version !tx))))))

(t/deftest handle-interrupted-exception-614
  (t/is (thrown-with-msg?
         Exception #"sleep interrupted"
         @(reduce conj []
                  (with-open [node (node/start-node {})]
                    (let [!tx1 @(c2/submit-tx node [[:put {:id :hello-world
                                                           :fn #c2/clj-form (fn hello-world [id]
                                                                              (sleep 200)
                                                                              [[:put {:id id :foo (str id)}]])}]])
                          !tx2 (c2/submit-tx node [[:call :hello-world 1]])]
                      (Thread/sleep 100)
                      (tu/then-await-tx !tx1 node)
                      (c2/plan-datalog-async node (assoc '{:find [id]
                                                           :where [[id :foo]]}
                                                         :basis {:tx !tx2}))))))))
