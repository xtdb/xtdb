(ns xtdb.remote-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.client.impl :as xtc]
            [xtdb.error :as err]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu :refer [*node*]])
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
        (recur (conj res (JsonSerde/decode ^String (first lns))) (rest lns))))))

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
                 {:basis {:at-tx #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}}})))

  (let [tx (xt/submit-tx *node* [(xt/put :docs {:xt/id 2 :key :some-keyword})])]
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

(defn- inst->str [^java.util.Date d] (str (.toInstant d)))

(def json-tx-ops
  [{"put" "docs"
    "doc" {"xt/id" 1}}
   {"put" "docs"
    "doc" {"xt/id" 2}}
   {"delete" "docs"
    "id" 1}
   {"put" "docs"
    "doc" {"xt/id" 3}
    "validFrom" (inst->str #inst "2050")
    "validTo" (inst->str #inst "2051")}
   {"erase" "docs"
    "id" 3}
   {"sql" "INSERT INTO docs (xt$id, bar, toto) VALUES (3, 1, 'toto')"}
   {"sql" "INSERT INTO docs (xt$id, bar, toto) VALUES (4, 1, 'toto')"}
   {"sql" "UPDATE docs SET bar = 2 WHERE docs.xt$id = 3"}
   {"sql" "DELETE FROM docs WHERE docs.bar = 2"}
   {"sql" "ERASE FROM docs WHERE docs.xt$id = 4"}])

(defn- tx-key->json-tx-key [{:keys [tx-id system-time] :as _tx-key}]
  {"txId" tx-id "systemTime" (str system-time)})

(deftest json-request-test
  (let [_tx1 (xt/submit-tx *node* [(xt/put :docs {:xt/id 1})])
        {:keys [tx-id system-time] :as tx2} #xt/tx-key {:tx-id 1, :system-time #time/instant "2020-01-02T00:00:00Z"}]

    (t/is (= tx2
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:txOps json-tx-ops}
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
                                              :queryOpts {:basis {:atTx (tx-key->json-tx-key tx2)}}}
                                :url (http-url "query")})
                 :body
                 decode-transit*))
          "testing query")

    (t/is (= [{:xt$id 2}]
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :json
                                :form-params {:query {"from" "docs", "bind" ["xt/id"]}
                                              :queryOpts {:basis {:atTx (tx-key->json-tx-key tx2)}
                                                          :keyFn "SQL_KW"}}
                                :url (http-url "query")})
                 :body
                 decode-transit*))
          "testing query opts")

    (t/testing "malformed tx request "
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :transit+json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:txOps [{"evict" "docs" "id" 3}]}
                           :url (http-url "tx")
                           :throw-exceptions? false})
            body (decode-transit body)]
        (t/is (= 400 status))
        (t/is (= "Illegal argument: 'xtql/malformed-tx-op'" (ex-message body)))
        (t/is (= {::err/error-key :xtql/malformed-tx-op}
                 (-> (ex-data body) (select-keys [::err/error-key]))))))

    (t/testing "malformed query request (see xt:instat)"
      (let [{:keys [status body] :as _resp}
            (http/request {:accept :transit+json
                           :as :string
                           :request-method :post
                           :content-type :json
                           :form-params {:query {"from" "docs", "bind" ["xt/id"]}
                                         :queryOpts {:basis {:atTx
                                                             {"tx-id" tx-id
                                                              "system-time" {"@type" "xt:instat" "@value" (str system-time)}}}}}
                           :url (http-url "query")
                           :throw-exceptions? false})
            body (decode-transit body)]
        (t/is (= 400 status))
        (t/is (= "Malformed \"application/json\" request." (ex-message body)))
        (t/is (= {:xtdb.error/error-key :malformed-request}
                 (-> (ex-data body) (select-keys [::err/error-key]))))))


    (t/testing "XTDml"
      (let [insert {"insertInto" "docs2" "query" {"from" "docs" "bind" ["xt/id"]}}
            tx3 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps [insert]}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]

        (t/is (= [{:xt/id 2}]
                 (xt/q *node* '(from :docs2 [xt/id version])
                       {:basis {:at-tx tx3}}))
              "insert-into"))

      (let [update {"op" {"updateTable" "docs2"
                          "bindSpecs" [{"xt/id" {"xt:param" "$uid"}}]
                          "setSpecs" [{"version" 3}]}
                    "args" [{"uid" 2}]}
            tx4 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps [update]}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]

        (t/is (= [{:xt/id 2 :version 3}]
                 (xt/q *node* '(from :docs2 [xt/id version])
                       {:basis {:at-tx tx4}}))
              "update-table"))

      (let [delete {"deleteFrom" "docs2"
                    "bindSpecs" [{"xt/id" 2}]}
            tx5 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps [delete]}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]
        (t/testing "delete-from"
          (t/is (= [] (xt/q *node* '(from :docs2 [xt/id version])
                            {:basis {:at-tx tx5}})))
          (t/is (= #{{:xt/id 2} {:xt/id 2 :version 3}}
                   (set (xt/q *node* '(from :docs2 {:bind [xt/id version]
                                                    :for-valid-time :all-time})
                              {:basis {:at-tx tx5}}))))))

      (xt/submit-tx tu/*node* [(xt/put :docs2 {:xt/id 3})])
      (let [erase {"eraseFrom" "docs2"
                   "bindSpecs" [{"xt/id" 3}]}
            tx6 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps [erase]}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]
        (t/is (= [] (xt/q *node* '(from :docs2 {:bind [{:xt/id 3} xt/id version]
                                                :for-valid-time :all-time})
                          {:basis {:at-tx tx6}}))
              "erase-from"))

      ;; using docs in combination with docs3
      (let [assert+put [{"assertExists" {"from" "docs", "bind" ["xt/id"]}}
                        {"put" "docs3" "doc" {"xt/id" 1}}]
            tx7 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps assert+put}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]
        (t/is (= [{:xt/id 1}] (xt/q *node* '(from :docs3 [xt/id])
                                    {:basis {:at-tx tx7}}))
              "assert-exists"))
      (let [assert+put [{"assertNotExists" {"from" "docs2", "bind" ["xt/id"]}}
                        {"delete" "docs3" "id" 1}]
            tx7 (-> (http/request {:accept :transit+json
                                   :as :string
                                   :request-method :post
                                   :content-type :json
                                   :form-params {:txOps assert+put}
                                   :url (http-url "tx")})
                    :body
                    decode-transit)]
        (t/is (= [] (xt/q *node* '(from :docs3 [xt/id])
                          {:basis {:at-tx tx7}}))
              "assert-not-exists")))))

(deftest json-both-ways-test
  (t/is (= [{"foo_bar" 1} {"foo_bar" 2}]
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :json
                              :form-params {:query {"rel" [{"foo-bar" 1} {"foo-bar" 2}] "bind" ["foo-bar"]}
                                            :queryOpts {:keyFn "SQL_STR"}}
                              :url (http-url "query")})
               :body
               decode-json*))))
