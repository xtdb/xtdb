(ns test-harness.core-test
  (:require [clojure.test :refer [deftest use-fixtures testing is]]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as http]
            [test-harness.core :as core]
            [test-harness.test-utils :as tu]))

(use-fixtures :each tu/with-system)

(deftest new-node!-test
  (let [node-store (::core/node-store @tu/system)]
    (is (core/new-node! node-store "token"))
    (is (= 1 (count @node-store)))
    (is (not (core/new-node! node-store "token")))
    (is (= 1 (count @node-store)))))

(deftest remove-node!-test
  (let [node-store (::core/node-store @tu/system)]
    (is (core/new-node! node-store "token"))
    (is (= 1 (count @node-store)))
    (is (core/remove-node! node-store "token"))
    (is (= 0 (count @node-store)))))

(deftest extract-token-test
  (is (= {:token "token"
          :uri "/query"}
        (core/extract-token "/token/query")))
  (is (nil? (core/extract-token "/query"))))

(defn- http-url [& parts]
  (str "http://localhost:" (get-in core/system [::core/server :port])
       (apply str parts)))

(defn setup []
  (let [response (http/get (http-url "/setup"))]
    (is (= 200 (:status response)))
    (:body response)))

(defn query [token body]
  (let [response (http/post (http-url "/" token "/query")
                            {:content-type :json
                             :body body
                             :as :json})]
    (is (= 200 (:status response)))
    (:body response)))

(defn tx [token body]
  (let [response (http/post (http-url "/" token "/tx")
                            {:content-type :json
                             :body body})]
    (is (= 200 (:status response)))
    response))

(deftest e2e-test
  (testing "Normal operation"
    (let [;; Create a new node
          token (setup)]

      ;; Query to show it's empty
      (is (= [] (query token "{\"sql\":\"SELECT * FROM foo\"}")))

      ;; Submit a transaction
      (tx token "{\"txOps\":[{\"sql\":\"INSERT INTO foo(xt$id, bar) VALUES (1, 'baz')\"}]}")

      ;; Query the results
      (is (= [{:xt$id 1 :bar "baz"}]
             (query token "{\"sql\":\"SELECT * FROM foo\"}")))

      ;; Tear down the node
      (let [response (http/get (http-url "/teardown?" token))]
        (is (= 200 (:status response))))

      ;; Get a 404 to show it's gone
      (let [response (http/post (http-url "/" token "/query")
                                {:content-type :json
                                 :body "{\"sql\":\"SELECT * FROM foo\"}"
                                 :throw-exceptions? false})]
        (is (= 404 (:status response))))))

  (testing "Two nodes at once"
    (let [token1 (setup)
          token2 (setup)]

      ;; Submit only to token1
      (tx token1 "{\"txOps\":[{\"sql\":\"INSERT INTO foo(xt$id, bar) VALUES (1, 'baz')\"}]}")

      (is (= [{:xt$id 1 :bar "baz"}]
             (query token1 "{\"sql\":\"SELECT * FROM foo\"}")))

      (is (= [] (query token2 "{\"sql\":\"SELECT * FROM foo\"}")))))

  (testing "Can set custom token"
    (let [token "my-custom-token"
          response (http/get (http-url "/setup?" token))]
      (is (= 200 (:status response)))

      (query token "{\"sql\":\"SELECT * FROM foo\"}"))))
