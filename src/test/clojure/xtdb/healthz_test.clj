(ns xtdb.healthz-test
  (:require [clj-http.client :as clj-http]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.healthz :as healthz]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(defn ->healthz-url [port endpoint]
  (format "http://localhost:%s/healthz/%s" port endpoint))

(defn ->system-url [port endpoint]
  (format "http://localhost:%s/system/%s" port endpoint))

(t/deftest test-healthz-endpoints
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [_node (tu/->local-node {:node-dir local-path
                                          :healthz-port port})]
        (t/testing "started endpoint"
          (let [resp (clj-http/get (->healthz-url port "started"))]
            (t/is (= 200 (:status resp)))
            (t/is (= {"X-XTDB-Target-Message-Id" "-1", "X-XTDB-Current-Message-Id" "-1"}
                     (-> (:headers resp)
                         (select-keys ["X-XTDB-Target-Message-Id" "X-XTDB-Current-Message-Id"]))))
            (t/is (= "Started." (:body resp)))))

        (t/testing "alive endpoint"
          (let [resp (clj-http/get (->healthz-url port "alive"))]
            (t/is (= 200 (:status resp)))
            (t/is (= "Alive." (:body resp)))))

        (t/testing "ready endpoint"
          (let [resp (clj-http/get (->healthz-url port "ready"))]
            (t/is (= 200 (:status resp)))
            (t/is (= "Ready." (:body resp)))))))))

(t/deftest test-started-catchup-4273
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [node (tu/->local-node {:node-dir local-path
                                         :compactor-threads 0})]
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1, :foo 1}]]
                       {:default-tz #xt/zone "Asia/Kolkata"})
        (tu/finish-block! node))

      (with-open [_node (tu/->local-node {:node-dir local-path
                                          :healthz-port port
                                          :compactor-threads 0})]
        (t/testing "started endpoint"
          (let [timeout-at-ms (+ (System/currentTimeMillis) 1000)]
            (loop []
              (when (< timeout-at-ms (System/currentTimeMillis))
                (throw (ex-info "timed out" {})))

              (let [resp (clj-http/get (->healthz-url port "started")
                                       {:throw-exceptions false})]

                (case (long (:status resp))
                  503 (do
                        (t/is (= {"X-XTDB-Target-Message-Id" "2097"}
                                 (-> (:headers resp)
                                     (select-keys ["X-XTDB-Target-Message-Id"]))))
                        (Thread/sleep 250)
                        (recur))
                  200 (do
                        (t/is (= {"X-XTDB-Target-Message-Id" "2137", "X-XTDB-Current-Message-Id" "2137"}
                                 (-> (:headers resp)
                                     (select-keys ["X-XTDB-Target-Message-Id" "X-XTDB-Current-Message-Id"]))))
                        (t/is (= "Started." (:body resp)))))))))))))

(t/deftest test-indexer-error
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [_node (tu/->local-node {:node-dir local-path
                                          :healthz-port port})]
        (t/testing "alive endpoint - non errored indexer"
          (t/is (= 200 (:status (clj-http/get (->healthz-url port "alive"))))))
        
        (t/testing "alive endpoint with indexer error"
          (with-redefs [healthz/get-ingestion-error (fn [_] (Exception. "Indexer error"))]
            (let [resp (clj-http/get (->healthz-url port "alive") {:throw-exceptions false})]
              (t/is (= 503 (:status resp)))
              (t/is (re-find #"Indexer error" (:body resp))))))))))

(t/deftest test-error-response
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [_node (tu/->local-node {:node-dir local-path
                                          :healthz-port port})]
        (t/testing "server thrown error responds with reasonable message"
          (with-redefs [healthz/get-ingestion-error (fn [_] (throw (Exception. "Some server error.")))]
            (let [resp (clj-http/get (->healthz-url port "alive") {:throw-exceptions false})]
              (t/is (= 500 (:status resp)))
              (t/is (re-find #"Exception when calling endpoint - java.lang.Exception: Some server error." (:body resp))))))))))

(t/deftest test-block-lag-healthy-4364
  (let [port (tu/free-port)]
    (with-open [_node (xtn/start-node {:log [:in-memory]
                                       :storage [:in-memory]
                                       :healthz {:port port}})]
      (t/testing "server thrown error responds with reasonable message"
        (letfn [(alive-resp []
                  (let [{:keys [status headers]} (clj-http/get (->healthz-url port "alive") {:throw-exceptions false})]
                    [status (select-keys headers ["X-XTDB-Block-Lag" "X-XTDB-Block-Lag-Healthy"])]))]
          (t/is (= [200 {"X-XTDB-Block-Lag" "0", "X-XTDB-Block-Lag-Healthy" "true"}]
                   (alive-resp)))

          (with-redefs [healthz/->block-lag (fn [_] 5)]
            (t/is (= [200 {"X-XTDB-Block-Lag" "5", "X-XTDB-Block-Lag-Healthy" "true"}]
                     (alive-resp))))

          (with-redefs [healthz/->block-lag (fn [_] 6)]
            (t/is (= [503 {"X-XTDB-Block-Lag" "6", "X-XTDB-Block-Lag-Healthy" "false"}]
                     (alive-resp)))))))))

(t/deftest test-finish-block-endpoint
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [node (tu/->local-node {:node-dir local-path
                                         :healthz-port port})]
        (let [block-cat (.getBlockCatalog (db/primary-db node))]

          (t/testing "no latest completed tx"
            (clj-http/post (->system-url port "finish-block") {:throw-exceptions false}))

          (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]])

          (t/is (= nil (:tx-id (.getLatestCompletedTx block-cat))))

          (let [first-latest-tx-id (:tx-id (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]
                                                                [:put-docs :bar {:xt/id "bar2"}]
                                                                [:put-docs :bar {:xt/id "bar3"}]]))]

            (t/testing "successful block flush"
              (let [resp (clj-http/post (->system-url port "finish-block") {:throw-exceptions false})]
                (t/is (= 200 (:status resp)))
                (t/is (= "Block flush message sent successfully." (:body resp)))))

            (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]])

            (t/is (= first-latest-tx-id (:tx-id (.getLatestCompletedTx block-cat)))))

          (let [second-latest-tx-id (:tx-id (xt/execute-tx node [[:put-docs :bar {:xt/id "bar7"}]
                                                                 [:put-docs :bar {:xt/id "bar8"}]
                                                                 [:put-docs :bar {:xt/id "bar9"}]]))]

            (t/testing "second successful block flush"
              (let [resp (clj-http/post (->system-url port "finish-block") {:throw-exceptions false})]
                (t/is (= 200 (:status resp)))
                (t/is (= "Block flush message sent successfully." (:body resp)))))

            (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]])

            (t/is (= second-latest-tx-id (:tx-id (.getLatestCompletedTx block-cat))))))))))
