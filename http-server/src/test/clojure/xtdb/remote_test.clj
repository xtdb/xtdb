(ns xtdb.remote-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.string :as str]
            [jsonista.core :as json]
            [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.client.impl :as xtc]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.jackson :as jackson]
            [xtdb.serde :as serde]
            [xtdb.error :as err])
  (:import (java.io ByteArrayInputStream EOFException)))

;; ONLY put stuff here where remote DIFFERS to in-memory

(t/use-fixtures :each tu/with-mock-clock tu/with-http-client-node)

(deftest normalisation-option
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Illegal argument: "
                          (xt/q *node* '(from :docs [first-name last-name])
                                {:key-fn identity}))
        "remote can not serialize arbitrary fns"))


(defn- http-url [endpoint] (str "http://localhost:" tu/*http-port* "/" endpoint))

(defn- decode-transit* [^String s]
  (let [rdr (transit/reader (ByteArrayInputStream. (.getBytes s)) :json {:handlers serde/transit-read-handlers})]
    (loop [res []]
      (let [value-read (try
                         (transit/read rdr)
                         (catch RuntimeException e
                           (if (instance? EOFException (.getCause e))
                             ::finished
                             (throw e))))]
        (if (= ::finished value-read)
          res
          (recur (conj res value-read)))))))

(defn- decode-transit [^String s] (first (decode-transit* s)))

(defn- decode-json* [^String s]
  (let [json-lines (str/split-lines s)]
    (loop [res [] lns json-lines]
      (if-not (seq lns)
        res
        (recur (conj res (json/read-value (first lns) jackson/json-ld-mapper)) (rest lns))))))

(defn- decode-json [^String s] (first (decode-json* s)))

(def possible-tx-ops
  [(xt/put :docs {:xt/id 1})
   (xt/put :docs {:xt/id 2})
   (xt/delete :docs 2)
   (-> (xt/put :docs {:xt/id 3})
       (xt/during #inst "2023" #inst "2024"))
   (xt/erase :docs 3)
   (xt/sql-op "INSERT INTO docs (xt$id, bar, toto) VALUES (3, 1, 'toto')")
   (xt/sql-op "INSERT INTO docs (xt$id, bar, toto) VALUES (4, 1, 'toto')")
   (xt/sql-op "UPDATE docs SET bar = 2 WHERE docs.xt$id = 3")
   (xt/sql-op "DELETE FROM docs WHERE docs.bar = 2")
   (xt/sql-op "ERASE FROM docs WHERE docs.xt$id = 4")])

(deftest transit-test
  (xt/submit-tx *node* [(xt/put :foo {:xt/id 1})])
  (Thread/sleep 100)

  (t/is (= {:latest-completed-tx
            #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-01T00:00:00Z"},
            :latest-submitted-tx
            #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-01T00:00:00Z"}}
           (-> (http/request {:accept :transit+json
                              :as :string
                              :request-method :get
                              :url (http-url "status")})
               :body
               decode-transit))
        "testing status")

  (t/is (= #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}
           (-> (http/request {:accept :transit+json
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {:tx-ops possible-tx-ops}
                              :transit-opts xtc/transit-opts
                              :url (http-url "tx")})
               :body
               decode-transit))
        "testing tx")

  (t/is (= [{:xt/id 1}]
           (xt/q *node* '(from :docs [xt/id])
                 {:basis {:at-tx #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}}})))

  (let [tx (xt/submit-tx *node* [(xt/put :docs {:xt/id 2})])]
    (t/is (= #{{:xt/id 1} {:xt/id 2}}
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :transit+json
                                :form-params {:query '(from :docs [xt/id])
                                              :basis {:at-tx tx}}
                                :transit-opts xtc/transit-opts
                                :url (http-url "query")})
                 :body
                 decode-transit*
                 set))
          "testing query")))

(deftest json-response-test
  (xt/submit-tx *node* [(xt/put :foo {:xt/id 1})])
  (Thread/sleep 100)

  (t/is (= {:latest-submitted-tx {:txId 0, :systemTime #time/instant "2020-01-01T00:00:00Z"},
            :latest-completed-tx {:txId 0, :systemTime #time/instant "2020-01-01T00:00:00Z"}}
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :get
                              :url (http-url "status")})
               :body
               decode-json))
        "testing status")

  (t/is (= {:txId 1, :systemTime #time/instant "2020-01-02T00:00:00Z"}
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
                 {:basis {:at-tx #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}}})))
  (let [tx (xt/submit-tx *node* [(xt/put :docs {:xt/id 2 :key :some-keyword})])]
    (t/is (= #{{:xt/id 1} {:xt/id 2 :key :some-keyword}}
             (-> (http/request {:accept "application/jsonl"
                                :as :string
                                :request-method :post
                                :content-type :transit+json
                                :form-params {:query '(from :docs [xt/id key])
                                              :basis {:at-tx tx}}
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
      (t/is (= "Illegal argument: ':xtql/malformed-table'" (ex-message body)))
      (t/is (= {:xtdb.error/error-type :illegal-argument,
                :xtdb.error/error-key :xtql/malformed-table,
                :xtdb.error/message "Illegal argument: ':xtql/malformed-table'",
                :table "docs",
                :from ["from" "docs" ["name"]]} (ex-data body)))))

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
      (t/is (= {:xtdb.error/error-type :runtime-error,
                :xtdb.error/error-key :xtdb.expression/division-by-zero,
                :xtdb.error/message "data exception — division by zero"}
               (ex-data body)))))

  (t/testing "unknown runtime error"
    (let [tx (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id 1 :name 2})])
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
      (t/is (= "No method in multimethod 'codegen-call' for dispatch value: [:upper :absent]"
               (ex-message body)))
      (t/is (= {:xtdb.error/error-type :unknown-runtime-error,
                :class "java.lang.IllegalArgumentException",
                :stringified "java.lang.IllegalArgumentException: No method in multimethod 'codegen-call' for dispatch value: [:upper :absent]"}
               (ex-data body))))))

(defn- inst->json-ld [^java.util.Date d]
  {"@type" "xt:instant" "@value" (str (.toInstant d))})

(def json-tx-ops
  [{"put" "docs"
    "doc" {"xt/id" 1}}
   {"put" "docs"
    "doc" {"xt/id" 2}}
   {"delete" "docs"
    "id" 1}
   {"put" "docs"
    "doc" {"xt/id" 3}
    "valid_from" (inst->json-ld #inst "2050")
    "valid_to" (inst->json-ld #inst "2051")}
   {"erase" "docs"
    "id" 3}

   #_#_#_#_#_ ;TODO deserializer for sql
   [:sql "INSERT INTO docs (xt$id, bar, toto) VALUES (3, 1, 'toto')"]
   [:sql "INSERT INTO docs (xt$id, bar, toto) VALUES (4, 1, 'toto')"]
   [:sql "UPDATE docs SET bar = 2 WHERE docs.xt$id = 3"]
   [:sql "DELETE FROM docs WHERE docs.bar = 2"]
   [:sql "ERASE FROM docs WHERE docs.xt$id = 4"]])

(defn- tx-key->json-tx-key [{:keys [tx-id system-time] :as _tx-key}]
  {"tx_id" tx-id "system_time" {"@type" "xt:instant"
                                "@value" (str system-time)}})

(deftest json-request-test
  (let [_tx1 (xt/submit-tx *node* [(xt/put :docs {:xt/id 1})])
        {:keys [tx-id system-time] :as tx2} #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}]

    (t/is (= tx2
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:tx_ops json-tx-ops}
                                :url (http-url "tx")})
                 :body
                 decode-transit))
          "testing tx")

    (t/is (= [{:xt/id 2}]
             (xt/q *node* '(from :docs [xt/id])
                   {:basis {:at-tx tx2}})))


    (t/is (= [{:xt/id 2}]
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:query {"from" "docs", "bind" ["xt/id"]}
                                              :query_opts {:basis {:at_tx (tx-key->json-tx-key tx2)}}}
                                :url (http-url "query")})
                 :body
                 decode-transit*
                 ))
          "testing query")

    (t/is (= [{:xt$id 2}]
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:query {"from" "docs", "bind" ["xt/id"]}
                                              :query_opts {:basis {:at_tx (tx-key->json-tx-key tx2)}
                                                           :key_fn "sql"}}
                                :url (http-url "query")})
                 :body
                 decode-transit*
                 ))
          "testing query opts")

    (t/testing "malformed tx request "
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :transit+json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:tx_ops [{"evict" "docs" "id" 3}]}
                           :url (http-url "tx")
                           :throw-exceptions? false})
            body (decode-transit body)]
        (t/is (= 400 status))
        (t/is (= "Illegal argument: ':xtql/malformed-tx-op'" (ex-message body)))
        (t/is (= {:xtdb.error/error-type :illegal-argument
                  :xtdb.error/error-key :xtql/malformed-tx-op
                  :xtdb.error/message "Illegal argument: ':xtql/malformed-tx-op'"}
                 (-> (ex-data body) (select-keys [::err/error-type ::err/error-key ::err/message]))))))

    (t/testing "malformed query request (see xt:instat)"
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :transit+json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:query {"from" "docs", "bind" ["xt/id"]}
                                         :query_opts {:basis {:at_tx
                                                              {"tx-id" tx-id
                                                               "system-time" {"@type" "xt:instat" "@value" (str system-time)}}}}}
                           :url (http-url "query")
                           :throw-exceptions? false})
            body (decode-transit body)]
        (t/is (= 400 status))
        (t/is (= "Illegal argument: ':xtql/malformed-tx-key'" (ex-message body)))
        (t/is (= {:xtdb.error/error-type :illegal-argument
                  :xtdb.error/error-key :xtql/malformed-tx-key
                  :xtdb.error/message "Illegal argument: ':xtql/malformed-tx-key'"}
                 (-> (ex-data body) (select-keys [::err/error-type ::err/error-key ::err/message]))))))))

(deftest json-both-ways-test
  (t/is (= [{:foo_bar 1} {:foo_bar 2}]
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :json
                              :form-params {:query {"rel" [{"foo-bar" 1} {"foo-bar" 2}] "bind" ["foo-bar"]}
                                            :query_opts {:key_fn "sql"}}
                              :url (http-url "query")})
               :body
               decode-json*))))
