(ns xtdb.remote-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.client.impl :as xtc]
            [xtdb.error :as err]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.tx-ops :as tx-ops])
  (:import (java.io ByteArrayInputStream EOFException)
           (xtdb JsonSerde)))

;; ONLY put stuff here where remote DIFFERS to in-memory

(t/use-fixtures :each tu/with-mock-clock tu/with-http-client-node)

#_ ; TODO accept arbitrary normalisation fns again
(deftest normalisation-option
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Illegal argument: "
                          (xt/q *node* '(from :docs [first-name last-name])
                                {:key-fn identity}))
        "remote can not serialize arbitrary fns"))

(defn- http-url [endpoint] (str "http://localhost:" tu/*http-port* "/" endpoint))

(defn- decode-json* [^String s]
  (let [json-lines (str/split-lines s)]
    (loop [res [] lns json-lines]
      (if-not (seq lns)
        res
        (recur (conj res (JsonSerde/decode ^String (first lns))) (rest lns))))))

(defn- decode-json [^String s] (first (decode-json* s)))

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
  (xt/submit-tx *node* [[:put-docs :foo {:xt/id 1}]])
  (Thread/sleep 100)

  (t/is (= {"latestSubmittedTx" {"txId" 0, "systemTime" "2020-01-01T00:00:00Z"},
            "latestCompletedTx" {"txId" 0, "systemTime" "2020-01-01T00:00:00Z"}}
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :get
                              :url (http-url "status")})
               :body
               decode-json))
        "testing status")

  (t/is (= {"txId" 1, "systemTime" "2020-01-02T00:00:00Z"}
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {:tx-ops possible-tx-ops}
                              :transit-opts xtc/transit-opts
                              :url (http-url "tx")})
               :body
               decode-json))
        "testing tx")

  (t/is (= [{:xt/id 1}]
           (xt/q *node* '(from :docs [xt/id])
                 {:basis {:at-tx #xt/tx-key {:tx-id 1, :system-time #xt.time/instant "2020-01-02T00:00:00Z"}}})))

  (let [tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id 2 :key :some-keyword}]])]
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
                                            :basis {:at-tx tx}
                                            :key-fn #xt/key-fn :kebab-case-keyword}
                              :transit-opts xtc/transit-opts
                              :url (http-url "query")})
               :body
               decode-json*
               set))
          "testing query"))

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
      (t/is (instance? xtdb.IllegalArgumentException body))
      (t/is (= "Illegal argument: 'xtql/malformed-table'" (ex-message body)))
      (t/is (= '{:xtdb.error/error-key :xtql/malformed-table,
                 :table docs,
                 :from [from docs [name]]} (ex-data body)))))


  (t/testing "runtime error"
    (let [{:keys [status body] :as _resp} (http/request {:accept "application/jsonl"
                                                         :as :string
                                                         :request-method :post
                                                         :content-type :transit+json
                                                         :form-params {:query '(-> (rel [{}] [])
                                                                                   (with {:foo (/ 1 0)}))}
                                                         :transit-opts xtc/transit-opts
                                                         :url (http-url "query")})
          body (decode-json body)]
      (t/is (= 200 status))
      (t/is (instance? xtdb.RuntimeException body))
      (t/is (= "data exception — division by zero" (ex-message body)))
      (t/is (= {:xtdb.error/error-key :xtdb.expression/division-by-zero}
               (ex-data body)))))

  ;; We shouldn't be able to produce deterministically an unknown runtime error.
  #_
  (t/testing "unknown runtime error"
    (let [tx (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :name 2}]])
          {:keys [status body] :as _resp} (http/request {:accept "application/jsonl"
                                                         :as :string
                                                         :request-method :post
                                                         :content-type :transit+json
                                                         :form-params {:query "SELECT UPPER(docs.name) AS name FROM docs"
                                                                       :basis {:at-tx tx}}
                                                         :transit-opts xtc/transit-opts
                                                         :url (http-url "query")
                                                         :throw-exceptions? false})
          body (decode-json body)]
      (t/is (= 500 status))
      (t/is (= "No method in multimethod 'codegen-call' for dispatch value: [:upper :i64]"
               (ex-message body)))
      (t/is (= {:xtdb.error/error-type :unknown-runtime-error,
                :class "java.lang.IllegalArgumentException",
                :stringified "java.lang.IllegalArgumentException: No method in multimethod 'codegen-call' for dispatch value: [:upper :i64]"}
               (ex-data body))))))

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

(defn- tx-key->json-tx-key [{:keys [tx-id system-time] :as _tx-key}]
  {"txId" tx-id "systemTime" (str system-time)})

(deftest json-request-test
  (let [_tx1 (xt/submit-tx *node* [[:put-docs :docs {:xt/id 1}]])
        {:keys [tx-id system-time] :as tx2} #xt/tx-key {:tx-id 1, :system-time #xt.time/instant "2020-01-02T00:00:00Z"}]

    (t/is (= {"txId" 1, "systemTime" "2020-01-02T00:00:00Z"}
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
             (xt/q *node* '(from :docs [xt/id])
                   {:after-tx tx2})))

    (t/is (= [{"xt/id" 2}]
             (-> (http/request {:accept :json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:sql "SELECT docs._id FROM docs"
                                              :queryOpts {:basis {:atTx (tx-key->json-tx-key tx2)}
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
        (t/is (= "Error decoding JSON!" (ex-message body)))
        (t/is (= {:xtdb.error/error-key :malformed-request}
                 (-> (ex-data body) (select-keys [::err/error-key]))))))

    (t/testing "malformed query request (see xt:instat)"
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:sql "SELECT _id FROM docs"
                                         :queryOpts {:basis {:atTx
                                                             {"tx-id" tx-id
                                                              "system-time" {"@type" "xt:instat" "@value" (str system-time)}}}}}
                           :url (http-url "query")
                           :throw-exceptions? false})
            body (decode-json body)]
        (t/is (= 400 status))
        (t/is (= "Error decoding JSON!" (ex-message body)))
        (t/is (= {:xtdb.error/error-key :malformed-request}
                 (-> (ex-data body) (select-keys [::err/error-key]))))))))

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
  (t/is (= [{"_column1" 1, "_column2" 3}]
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :json
                              :form-params {:sql "SELECT LEAST(?,2), LEAST(3,4) FROM (VALUES (1)) x"
                                            :queryOpts {:args [1]
                                                        :keyFn "CAMEL_CASE_STRING"}}
                              :url (http-url "query")})
               :body
               decode-json*))
        "testing sql query with args"))
