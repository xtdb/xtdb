(ns xtdb.healthz-test
  (:require
   [clj-http.client :as clj-http]
   [clojure.test :as t]
   [xtdb.healthz :as healthz]
   [xtdb.test-util :as tu]
   [xtdb.util :as util]))

(defn ->healthz-url [port endpoint]
  (format "http://localhost:%s/healthz/%s" port endpoint))

(t/deftest test-healthz-endpoints
  (util/with-tmp-dirs #{local-path}
    (let [port (tu/free-port)]
      (with-open [_node (tu/->local-node {:node-dir local-path
                                          :healthz-port port})]
        (t/testing "started endpoint"
          (let [resp (clj-http/get (->healthz-url port "started"))]
            (t/is (= 200 (:status resp)))
            (t/is (= "Started." (:body resp)))))
        (t/testing "alive endpoint"
          (let [resp (clj-http/get (->healthz-url port "alive"))]
            (t/is (= 200 (:status resp)))
            (t/is (= "Alive." (:body resp)))))
        (t/testing "ready endpoint"
          (let [resp (clj-http/get (->healthz-url port "ready"))]
            (t/is (= 200 (:status resp)))
            (t/is (= "Ready." (:body resp)))))))))

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
