(ns xtdb.tx-document-store-safety-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.fixtures :as f]
            [xtdb.io :as xio]
            [xtdb.kv.document-store :as kv-doc-store]
            [xtdb.mem-kv :as mem-kv]
            [xtdb.cache.nop :as nop-cache]
            [xtdb.tx-document-store-safety :as tx-doc-store-safety])
  (:import (clojure.lang IFn)
           (java.io Closeable)))

(defn dodgy-doc-store []
  (let [doc-store (kv-doc-store/->document-store {:kv-store (mem-kv/->kv-store)
                                                  :document-cache (nop-cache/->nop-cache {})})
        fetch-ex (atom nil)]
    (reify db/DocumentStore
      (submit-docs [_ id-and-docs]
        (db/submit-docs doc-store id-and-docs))
      (fetch-docs [_ ids]
        (when-some [ex @fetch-ex]
          (throw ex))
        (db/fetch-docs doc-store ids))
      IFn
      (invoke [_ ex] (reset! fetch-ex ex))
      Closeable
      (close [_] (xio/try-close doc-store)))))

(defn misbehave [node ex]
  (let [doc-store (:xtdb/document-store @(:!system node))]
    (doc-store ex)))

(defn behave [node]
  (let [doc-store (:xtdb/document-store @(:!system node))]
    (doc-store nil)))

(def test-tx-fn-state (atom {}))

(defn run-test-tx-fn [fid args]
  (if-some [{:keys [f, on-start, on-ret, on-ex, on-done]} (get @test-tx-fn-state fid)]
    (try
      (on-start)
      (try
        (doto (apply f args) on-ret)
        (catch Throwable t (on-ex t) (throw t)))
      (finally (on-done)))
    (throw (Exception. "Unexpected tx fn"))))

(defn test-tx-fn [{:keys [node, f, args, on-start, on-ret, on-ex, on-done]
                   :or {on-start (constantly nil)
                        on-ret (constantly nil)
                        on-ex (constantly nil)
                        on-done (constantly nil)}}]
  (let [fid (str (random-uuid))
        f (eval f)
        f' '(fn [ctx fid & args] ((requiring-resolve 'xtdb.tx-document-store-safety-test/run-test-tx-fn) fid (cons ctx args)))
        ret-ref (atom nil)
        ex-ref (atom nil)
        done-promise (promise)]
    (->> {:f f
          :on-start on-start
          :on-ret (fn [ret] (reset! ret-ref ret) (on-ret ret))
          :on-ex (fn [ex] (reset! ex-ref ex) (on-ex ex))
          :on-done (fn []
                     (deliver done-promise {:ret @ret-ref, :ex @ex-ref})
                     (swap! test-tx-fn-state dissoc fid)
                     (on-done))}
         (swap! test-tx-fn-state assoc fid))
    (xt/submit-tx node [[::xt/put {:xt/id fid, :xt/fn f'}] (into [::xt/fn fid fid] args)])
    done-promise))

(defn dodgy-node ^Closeable []
  (xt/start-node {:xtdb/document-store {:xtdb/module (fn [_] (dodgy-doc-store))}}))

(def recoverable-exceptions
  [{:t Exception}
   {:cognitect.anomalies/category :cognitect.anomalies/busy}
   {:cognitect.anomalies/category :cognitect.anomalies/unavailable}])

(def unrecoverable-exceptions
  [{:t Error}
   {:cognitect.anomalies/category nil}
   {:cognitect.anomalies/category :cognitect.anomalies/forbidden}
   {:cognitect.anomalies/category :cognitect.anomalies/not-a-cat}])

(def all-exceptions
  (concat recoverable-exceptions unrecoverable-exceptions))

(defn construct-ex [{:keys [t, cognitect.anomalies/category]} msg]
  (condp = t
    Exception (Exception. (str msg))
    Error (Error. (str msg))
    (ex-info msg {:cognitect.anomalies/category category})))

(defn ingester-panic? [node] (-> node :!system deref :xtdb/tx-ingester :!error deref some?))

(t/deftest all-exception-examples-rethrow-for-plain-query-test
  (doseq [example all-exceptions
          :let [ex (construct-ex example "boom")]]
    (with-open [node (dodgy-node)]
      (xt/submit-tx node [[::xt/put {:xt/id 0, :n 42}]])
      (xt/sync node)
      (t/is (= #{[{:n 42}]} (xt/q (xt/db node) '{:find [(pull ?e [:n])] :where [[?e :n]]})))
      (misbehave node ex)
      (t/is (thrown-with-msg? Throwable #"boom" (xt/q (xt/db node) '{:find [(pull ?e [:n])] :where [[?e :n]]}))))))

(t/deftest reads-during-tx-processing-recovery-test
  (doseq [example recoverable-exceptions
          :let [unique-msg (str (random-uuid))
                ex (construct-ex example unique-msg)
                retried (promise)]]
    (with-open [node (dodgy-node)]
      (misbehave node ex)
      (with-redefs [xio/sleep (constantly nil)
                    tx-doc-store-safety/*exp-backoff-opts*
                    (assoc tx-doc-store-safety/*exp-backoff-opts*
                      :on-retry (fn [ex] (when (= unique-msg (ex-message ex)) (deliver retried true))))]
        (xt/submit-tx node [[::xt/put {:xt/id unique-msg, :n 0}]])
        (t/is (f/spin-until-true 100 #(realized? retried)))
        (behave node)
        (t/is (f/spin-until-true 100 #(xt/entity (xt/db node) unique-msg)))))))

(t/deftest reads-during-tx-processing-panic-test
  (doseq [example unrecoverable-exceptions
          :let [unique-msg (str (random-uuid))
                ex (construct-ex example "test panic")]]
    (with-open [node (dodgy-node)]
      (misbehave node ex)
      (with-redefs [xio/sleep (constantly nil)]
        (xt/submit-tx node [[::xt/put {:xt/id unique-msg, :n 0}]])
        (t/is (f/spin-until-true 100 #(ingester-panic? node)))))))

(t/deftest entity-or-pull-call-during-tx-fn-processing-recovery-test
  (doseq [example recoverable-exceptions
          call ['(xtdb.api/entity (xtdb.api/db ctx) 0)
                '(xtdb.api/pull (xtdb.api/db ctx) [:foo] 0)
                '(xtdb.api/q (xtdb.api/db ctx) (quote {:find [(pull ?e [:foo])] :where [[?e :xt/id 0]]}))]]
    (let [unique-msg (str (random-uuid))
          ex (construct-ex example unique-msg)
          retried (promise)]
      (with-open [node (dodgy-node)]
        (with-redefs [xio/sleep (constantly nil)
                      tx-doc-store-safety/*exp-backoff-opts*
                      (assoc tx-doc-store-safety/*exp-backoff-opts*
                        :on-retry (fn [ex]
                                    (when (= unique-msg (ex-message ex))
                                      (deliver retried true))))]
          (xt/submit-tx node [[::xt/put {:xt/id 0}]])
          (test-tx-fn {:node node
                       :f (list 'fn '[ctx unique-msg] call '[[:xtdb.api/put {:xt/id unique-msg}]])
                       :args [unique-msg]
                       :on-start (fn [] (misbehave node ex))})
          (t/is (f/spin-until-true 100 #(realized? retried)))
          (behave node)
          (t/is (f/spin-until-true 100 #(xt/entity (xt/db node) unique-msg))))))))

(t/deftest entity-during-tx-fn-tolerates-missing-test
  (with-open [node (xt/start-node {})]
    (xt/submit-tx node [[::xt/put {:xt/id 0}]])
    (test-tx-fn {:node node
                 :f '(fn [ctx]
                       (when (nil? (xtdb.api/entity (xtdb.api/db ctx) "foo"))
                         [[:xtdb.api/put {:xt/id 1}]]))})
    (t/is (f/spin-until-true 100 #(xt/entity (xt/db node) 1)))))

(t/deftest pull-during-tx-fn-tolerates-missing-test
  (with-open [node (xt/start-node {})]
    (xt/submit-tx node [[::xt/put {:xt/id 0, :x "foo"}]])
    (test-tx-fn {:node node
                 :f '(fn [ctx]
                       (when (= #{[nil]} (xtdb.api/q (xtdb.api/db ctx) (quote {:find [(pull ?x [:xt/id])] :where [[?e :x ?x]]})))
                         [[:xtdb.api/put {:xt/id 1}]]))})
    (t/is (f/spin-until-true 100 #(xt/entity (xt/db node) 1)))))
