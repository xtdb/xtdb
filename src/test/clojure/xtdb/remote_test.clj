(ns xtdb.remote-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.string :as str]
            [jsonista.core :as json]
            [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.client.impl :as xtc]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.serde :as serde])
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
        (recur (conj res (json/read-value (first lns))) (rest lns))))))

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

  (Thread/sleep 100)

  (t/is (= [{:xt/id 1}]
           (xt/q *node* '(from :docs [xt/id]))))

  (let [tx (xt/submit-tx *node* [(xt/put :docs {:xt/id 2})])]
    (t/is (= #{{:xt/id 1} {:xt/id 2}}
             (-> (http/request {:accept :transit+json
                                :as :string
                                :request-method :post
                                :content-type :transit+json
                                :form-params {:query '(from :docs [xt/id])
                                              :basis {:tx tx}}
                                :transit-opts xtc/transit-opts
                                :url (http-url "query")})
                 :body
                 decode-transit*
                 set))
          "testing query")))

(deftest json-repsonse-test
  (xt/submit-tx *node* [(xt/put :foo {:xt/id 1})])
  (Thread/sleep 100)

  (t/is (= {"latest-completed-tx"
            {"tx-id" 0, "system-time" "2020-01-01T00:00:00Z"},
            "latest-submitted-tx"
            {"tx-id" 0, "system-time" "2020-01-01T00:00:00Z"}}
           (-> (http/request {:accept :json
                              :as :string
                              :request-method :get
                              :url (http-url "status")})
               :body
               decode-json))
        "testing status")

  (t/is (= {"tx-id" 1, "system-time" "2020-01-02T00:00:00Z"}
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

  (Thread/sleep 100)
  (t/is (= [{:xt/id 1}]
           (xt/q *node* '(from :docs [xt/id]))))

  (let [tx (xt/submit-tx *node* [(xt/put :docs {:xt/id 2})])]
    (t/is (= #{{"id" 2} {"id" 1}}
             (-> (http/request {:accept "application/jsonl"
                                :as :string
                                :request-method :post
                                :content-type :transit+json
                                :form-params {:query '(from :docs [xt/id])
                                              :basis {:tx tx}}
                                :transit-opts xtc/transit-opts
                                :url (http-url "query")})
                 :body
                 decode-json*
                 set))
          "testing query"))

  (t/is (= {:status 400,
            :body
            {"xtdb.error/error-type" "illegal-argument",
             "xtdb.error/error-key" "xtql/malformed-table",
             "xtdb.error/message" "Illegal argument: ':xtql/malformed-table'",
             "table" "docs",
             "from" ["from" "docs" ["name"]]}}
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {:query '(from docs [name])}
                              ;; :transit-opts xtc/transit-opts
                              :url (http-url "query")
                              :throw-exceptions? false}
                             #_#_identity identity)
               (update :body decode-json)
               (select-keys [:status :body])))
        "illegal argument error")

  ;; TODO how to mark this as an error
  (t/is (= {:status 200,
            :body
            [{"error-type" "runtime-error",
              "error-key" "division-by-zero",
              "message" "data exception — division by zero"}]}
           (-> (http/request {:accept "application/jsonl"
                              :as :string
                              :request-method :post
                              :content-type :transit+json
                              :form-params {:query '(-> (rel [{}] [])
                                                        (with {:foo (/ 1 0)}))}
                              :transit-opts xtc/transit-opts
                              :url (http-url "query")
                              :throw-exceptions? false})
               (update :body decode-json*)
               (select-keys [:status :body])))
        "illegal argument error")

  (let [tx (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id 1 :name 2})])]
    (t/is (= {:status 500,
              :body
              {"xtdb.error/error-type" "unknown-runtime-error",
               "class" "java.lang.IllegalArgumentException",
               "stringified"
               "java.lang.IllegalArgumentException: No method in multimethod 'codegen-call' for dispatch value: [:upper :absent]"}}
             (-> (http/request {:accept "application/jsonl"
                                :as :string
                                :request-method :post
                                :content-type :transit+json
                                :form-params {:query "SELECT UPPER(docs.name) AS name FROM docs"
                                              :basis {:tx tx}}
                                :transit-opts xtc/transit-opts
                                :url (http-url "query")
                                :throw-exceptions? false})
                 (update :body decode-json)
                 (select-keys [:status :body])))
          "unknown error")))
