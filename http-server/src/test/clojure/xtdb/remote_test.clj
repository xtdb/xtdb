(ns xtdb.remote-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.server :as xt-http]
            [xtdb.test-util :as tu]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util])
  (:import (xtdb JsonSerde)
           (xtdb.error Incorrect)))

;; ONLY put stuff here where remote DIFFERS to in-memory

(def ^:dynamic *http-port* nil)

(t/use-fixtures :each tu/with-mock-clock (tu/with-opts {:http-server {:port 0}}) tu/with-node
  (fn [f]
    (binding [*http-port* (xt-http/http-port tu/*node*)]
      (f))))

(defn- http-url [endpoint] (str "http://localhost:" *http-port* "/" endpoint))

(defn- decode-json* [^String s]
  (let [json-lines (str/split-lines s)]
    (loop [res [] lns json-lines]
      (if-not (seq lns)
        res
        (recur (conj res (JsonSerde/decode ^String (first lns))) (rest lns))))))

(defn- decode-json [^String s] (first (decode-json* s)))

(def transit-opts
  {:decode {:handlers serde/transit-read-handler-map}
   :encode {:handlers serde/transit-write-handler-map}})

(def possible-tx-ops
  (->> [[:put-docs :docs {:xt/id 1}]
        [:put-docs :docs {:xt/id 2}]
        [:delete-docs :docs 2]
        [:put-docs {:into :docs, :valid-from #inst "2023", :valid-to #inst "2024"}
         {:xt/id 3}]
        [:erase-docs :docs 3]
        [:sql "INSERT INTO docs (_id, bar, toto) VALUES (3, 1, 'toto')"]
        [:sql "INSERT INTO docs (_id, bar, toto) VALUES (4, 1, 'toto')"]
        [:sql "UPDATE docs SET bar = 2 WHERE docs._id = 3"]
        [:sql "DELETE FROM docs WHERE docs.bar = 2"]
        [:sql "ERASE FROM docs WHERE docs._id = 4"]]
       (mapv tx-ops/parse-tx-op)))

(deftest json-response-test
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (Thread/sleep 100)

  (t/is (= {"latestSubmittedTxId" 0,
            "latestCompletedTx" {"txId" 0, "systemTime" "2020-01-01T00:00:00Z"}}
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {}
                              :url (http-url "status")})
               :body
               decode-json))
        "testing status")

  (t/is (= 1
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {:tx-ops possible-tx-ops}
                              :transit-opts transit-opts
                              :url (http-url "tx")})
               :body
               decode-json))
        "testing tx")

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* '(from :docs [xt/id])
                 {:snapshot-time #xt/zdt "2020-01-02Z"
                  :after-tx-id 1})))

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 2 :key :some-keyword}]])

  (t/is (=
         ;; TODO figure out clojure bug
         #_#{{"xt/id" 1} {"xt/id" 2 "key" :some-keyword}}
         (java.util.Set/of (java.util.Map/of "xt/id" 1)
                           (java.util.Map/of "xt/id" 2 "key" :some-keyword))
         (-> (http/request {:accept "application/jsonl"
                            :as :string
                            :request-method :post
                            :content-type :transit+json
                            :form-params {:query '(from :docs [xt/id key])
                                          :after-tx-id 2
                                          :key-fn #xt/key-fn :kebab-case-keyword}
                            :transit-opts transit-opts
                            :url (http-url "query")})
             :body
             decode-json*
             set))
        "testing query")

  (t/testing "illegal argument error"
    (let [{:keys [status body] :as _resp} (http/request {:accept "application/jsonl"
                                                         :as :string
                                                         :request-method :post
                                                         :content-type :transit+json
                                                         :form-params {:query '(from docs [name])}
                                                         :url (http-url "query")
                                                         :throw-exceptions? false})
          body (decode-json body)]
      (t/is (= 400 status))
      (t/is (anomalous? [:incorrect :xtql/malformed-table
                         '{:table docs, :from [from docs [name]]}]
                        (throw body)))))


  (t/testing "runtime error"
    (let [{:keys [status body] :as _resp} (http/request {:accept "application/jsonl"
                                                         :as :string
                                                         :request-method :post
                                                         :content-type :transit+json
                                                         :form-params {:query '(-> (rel [{}] [])
                                                                                   (with {:foo (/ 1 0)}))}
                                                         :transit-opts transit-opts
                                                         :url (http-url "query")})
          body (decode-json body)]
      (t/is (= 200 status))
      (t/is (anomalous? [:incorrect :xtdb.expression/division-by-zero
                         "data exception - division by zero"]
                        (throw body))))))

(def json-tx-ops
  [{"sql" "INSERT INTO docs (_id) VALUES (1)"}
   {"sql" "INSERT INTO docs (_id) VALUES (2)"}
   {"sql" "DELETE FROM docs WHERE _id = 1"}
   {"sql" "INSERT INTO docs (_id, _valid_from, _valid_to) VALUES (3, DATE '2050-01-01', DATE '2051-01-01')"}
   {"sql" "ERASE FROM docs WHERE _id = 3"}
   {"sql" "INSERT INTO docs (_id, bar, toto) VALUES (3, 1, 'toto')"}
   {"sql" "INSERT INTO docs (_id, bar, toto) VALUES (4, 1, 'toto')"}
   {"sql" "UPDATE docs SET bar = 2 WHERE docs._id = 3"}
   {"sql" "DELETE FROM docs WHERE docs.bar = 2"}
   {"sql" "ERASE FROM docs WHERE docs._id = 4"}])

(deftest json-request-test
  (let [_tx1 (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])
        {:keys [tx-id system-time]} #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}]

    (t/is (= 1
             (-> (http/request {:accept :json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:txOps json-tx-ops}
                                :url (http-url "tx")})
                 :body
                 decode-json))
          "testing tx")

    (t/is (= [{:xt/id 2}]
             (xt/q tu/*node* '(from :docs [xt/id])
                   {:after-tx-id tx-id})))

    (t/is (= [{"xt/id" 2}]
             (-> (http/request {:accept :json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:sql "SELECT docs._id FROM docs"
                                              :queryOpts {:snapshotTime (str system-time)
                                                          :keyFn "KEBAB_CASE_KEYWORD"}}
                                :url (http-url "query")})
                 :body
                 decode-json))
          "testing sql query")

    (t/testing "malformed tx request "
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:txOps [{"sql" 1}]}
                           :url (http-url "tx")
                           :throw-exceptions? false})
            body (decode-json body)]
        (t/is (= 400 status))
        (t/is (anomalous? [:incorrect nil "Error decoding JSON"]
                          (throw body)))))

    (t/testing "malformed query request (see xt:instat)"
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:sql "SELECT _id FROM docs"
                                         :queryOpts {:atTx {"txId" tx-id
                                                            "systemTime" {"@type" "xt:instat" "@value" (str system-time)}}}}
                           :url (http-url "query")
                           :throw-exceptions? false})
            body (decode-json body)]
        (t/is (= 400 status))
        (t/is (anomalous? [:incorrect nil "Error decoding JSON"]
                          (throw body)))))))

(deftest json-both-ways-test
  (t/is (= [{"foo_bar" 1} {"foo_bar" 2}]
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :json
                              :form-params {:sql "SELECT * FROM (VALUES 1, 2) vals (foo_bar)"
                                            :queryOpts {:keyFn "SNAKE_CASE_STRING"}}
                              :url (http-url "query")})
               :body
               decode-json*))))

(deftest testing-sql-query-with-args-3167
  (t/is (= [{"_column_1" 1, "_column_2" 3}]
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :json
                              :form-params {:sql "SELECT LEAST(?,2), LEAST(3,4) FROM (VALUES (1)) x"
                                            :queryOpts {:args [1]
                                                        :keyFn "SNAKE_CASE_STRING"}}
                              :url (http-url "query")})
               :body
               decode-json*))
        "testing sql query with args"))

(deftest http-authentication
  (util/with-open [node (xtn/start-node {:http-server {:port 0}
                                         :authn [:user-table {:rules [{:user "xtdb" :method :password :address "127.0.0.1"}]}]})]
    (binding [*http-port* (xt-http/http-port node)]
      (t/testing "no authentication record"
        (let [{:keys [status]} (-> (http/post (http-url "status")
                                              {:accept :json
                                               :as :string
                                               :content-type :transit+json
                                               :form-params {}
                                               :transit-opts transit-opts
                                               :throw-exceptions? false}))]
          (t/is (= 401 status))))

      (t/is (= {"latestCompletedTx" nil, "latestSubmittedTxId" nil}
               (-> (http/post (http-url "status")
                              {:accept :json
                               :as :string
                               :content-type :transit+json
                               :basic-auth {:user "xtdb", :pass "xtdb"}
                               :form-params {}
                               :transit-opts transit-opts
                               :throw-exceptions? false})
                   :body
                   decode-json)))

      (t/is (= [{"x" 1}] (-> (http/post (http-url "query")
                                        {:accept :json
                                         :as :string
                                         :content-type :json
                                         :basic-auth {:user "xtdb", :pass "xtdb"}
                                         :form-params {:sql "SELECT 1 AS x"}})
                             :body
                             decode-json))
            "query auth with json")

      (t/is (= 0
               (-> (http/post (http-url "tx")
                              {:accept :json
                               :as :string
                               :content-type :json
                               :basic-auth {:user "xtdb", :pass "xtdb"}
                               :form-params {:txOps json-tx-ops}})
                   :body
                   decode-json))
            "transaction auth with json")

      (t/testing "wrong password"
        (let [{:keys [status body]} (-> (http/post (http-url "status")
                                                   {:accept :json
                                                    :as :string
                                                    :content-type :transit+json
                                                    :basic-auth {:user "xtdb", :pass "foobar"}
                                                    :transit-opts transit-opts
                                                    :throw-exceptions? false}))
              body (decode-json body)]
          (t/is (= 401 status))
          (t/is (= "password authentication failed for user: xtdb" (ex-message body)))))))

  (t/testing "users with a authentication record but not in the database"
    (util/with-open [node (xtn/start-node {:http-server {:port 0}
                                           :authn [:user-table {:rules [{:user "fin", :method :password, :address "127.0.0.1"}]}]})]
      (binding [*http-port* (xt-http/http-port node)]
        (let [{:keys [status body]} (-> (http/post (http-url "status")
                                                   {:accept :json
                                                    :as :string
                                                    :request-method :post
                                                    :content-type :transit+json
                                                    :basic-auth {:user "fin", :pass "foobar"}
                                                    :transit-opts transit-opts
                                                    :throw-exceptions? false}))

              body (decode-json body)]
          (t/is (= 401 status))
          (t/is (= "password authentication failed for user: fin" (ex-message body))))))))
