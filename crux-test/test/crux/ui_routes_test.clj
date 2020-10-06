(ns crux.ui-routes-test
  (:require [crux.fixtures :as fix :refer [*api*]]
            [clojure.edn :as edn]
            [clojure.test :as t]
            [crux.api :as crux]
            [clj-http.client :as http]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [jsonista.core :as json]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [crux.http-server.entity-ref :as entity-ref]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(t/use-fixtures :each
  fh/with-http-server
  fix/with-node)

(defn- parse-body [{:keys [^InputStream body]} content-type]
  (case content-type
    "application/transit+json" (transit/read (transit/reader body :json {:handlers {"crux.http/entity-ref" entity-ref/ref-read-handler}}))
    "application/json" (json/read-value body)
    "application/edn" (edn/read-string {:readers {'crux.http/entity-ref entity-ref/->EntityRef
                                                  'crux/id str}} (slurp body))
    "text/csv" (with-open [rdr (io/reader body)]
                 (doall (csv/read-csv rdr)))
    "text/tsv" (with-open [rdr (io/reader body)]
                 (doall (csv/read-csv rdr :separator \tab)))))

(defn- get-result-from-path
  ([path]
   (get-result-from-path path "application/edn"))
  ([path accept-type]
   (http/get (str *api-url* path)
             {:accept accept-type
              :as :stream
              :redirect-strategy :none})))

(t/deftest test-ui-routes
  ;; Insert data
  (let [{:keys [crux.tx/tx-id crux.tx/tx-time] :as tx} (-> (http/post (str *api-url* "/_crux/submit-tx")
                                                                      {:content-type :edn
                                                                       :body (pr-str {:tx-ops [[:crux.tx/put {:crux.db/id :ivan, :linking :peter}]
                                                                                               [:crux.tx/put {:crux.db/id :peter, :name "Peter"}]]})
                                                                       :as :stream})
                                                           (parse-body "application/edn"))]
    (http/get (str *api-url* "/_crux/await-tx?tx-id=" tx-id))

    ;; Test redirect on "/" endpoint.
    (t/is (= "/_crux/query" (-> (get-result-from-path "/")
                                (get-in [:headers "Location"]))))

    ;; Test getting the entity with different types
    (let [get-entity (fn [accept-type] (-> (get-result-from-path "/_crux/entity?eid-edn=:peter" accept-type)
                                           (parse-body accept-type)))]
      (t/is (= {:crux.db/id :peter, :name "Peter"}
               (get-entity "application/edn")))
      (t/is (= {:crux.db/id :peter, :name "Peter"}
               (get-entity "application/transit+json"))))

    ;; Test getting linked entities
    (let [get-linked-entities (fn [accept-type]
                                (-> (get-result-from-path "/_crux/entity?eid-edn=:ivan&link-entities?=true" accept-type)
                                    (parse-body accept-type)))]
      (t/is (= {:crux.db/id :ivan, :linking (entity-ref/->EntityRef :peter)}
               (get-linked-entities "application/edn")))
      (t/is (= {:crux.db/id :ivan, :linking (entity-ref/->EntityRef :peter)}
               (get-linked-entities "application/transit+json"))))

    ;; Testing getting query results (GET)
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path (format "/_crux/query?query=%s" '{:find [e] :where [[e :crux.db/id _]]}) accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[:ivan] [:peter]} (get-query "application/edn")))
      (t/is (= #{[:ivan] [:peter]} (get-query "application/transit+json")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/csv")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/tsv"))))

    ;; Testing getting query results (POST)
    (let [post-query (fn [{:keys [body content-type accept-type]}]
                       (let [accept-type (or accept-type content-type)]
                         (let [{:keys [status body] :as results} (http/post (str *api-url* "/_crux/query")
                                                                            {:accept accept-type
                                                                             :content-type content-type
                                                                             :as :stream
                                                                             :body body
                                                                             :throw-exceptions false})]

                           (cond-> (parse-body results accept-type)
                             (= status 200) set))))]
      (t/is (= #{[:ivan] [:peter]} (post-query
                                    {:body (pr-str {:query '{:find [e] :where [[e :crux.db/id _]]}})
                                     :content-type "application/edn"})))
      (t/is (= #{[:ivan] [:peter]} (post-query
                                    {:body "[\"^ \",\"~:query\",[\"^ \",\"~:find\",[\"~$e\"],\"~:where\",[[\"~$e\",\"~:crux.db/id\",\"~$_\"]]]]"
                                     :content-type "application/transit+json"})))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (post-query
                                              {:body (pr-str {:query '{:find [e] :where [[e :crux.db/id _]]}})
                                               :content-type "application/edn"
                                               :accept-type "text/csv"})))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (post-query
                                              {:body (pr-str {:query '{:find [e] :where [[e :crux.db/id _]]}})
                                               :content-type "application/edn"
                                               :accept-type "text/tsv"})))

      ;; Testing POSTing malformed queries
      (t/is (= {:error "Malformed \"application/edn\" request."}
               (post-query
                {:body "{:query {:find [e] :where [[e :crux.db/id _]}"
                 :content-type "application/edn"})))

      (t/is (= {:error "Malformed \"application/transit+json\" request."}
               (post-query
                {:body "[\"^ \",\"~:query\",[\"^ \",\"~:find\",[\"~$e\"],\"~:where\",[[\"~$e\",\"~:crux.db/id\",\"~$_\"]]]"
                 :content-type "application/transit+json"})))))

  ;; Testing getting linked entities in query results
  (let [get-query (fn [accept-type]
                    (set (-> (get-result-from-path (format "/_crux/query?query=%s&link-entities?=true" '{:find [e] :where [[e :crux.db/id _]]}) accept-type)
                             (parse-body accept-type))))]
    (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/edn")))
    (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/transit+json"))))

  ;; Test file-type based negotiation
  (t/is (= #{[":ivan"] [":peter"] ["e"]}
           (set (-> (get-result-from-path (format "/_crux/query.csv?query=%s" '{:find [e] :where [[e :crux.db/id _]]}))
                    (parse-body "text/csv")))))
  (t/is (= #{[":ivan"] [":peter"] ["e"]}
           (set (-> (get-result-from-path (format "/_crux/query.tsv?query=%s" '{:find [e] :where [[e :crux.db/id _]]}))
                    (parse-body "text/tsv"))))))

(t/deftest test-string-eid-routes
  (let [{:keys [crux.tx/tx-id] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id "string-id"}]])]
    (t/is (= {:crux.db/id "string-id"}
             (-> (get-result-from-path "/_crux/entity?eid=string-id" "application/edn")
                 (parse-body "application/edn"))))
    (t/is (= tx-id
             (-> (get-result-from-path "/_crux/entity-tx?eid=string-id" "application/edn")
                 (parse-body "application/edn")
                 :crux.tx/tx-id)))))

(t/deftest test-json-api
  (t/testing "JSON API routes"
    (let [json-get (fn [{:keys [url qps http-opts]}] (-> (http/get (str *api-url* url)
                                                                   (merge
                                                                    {:accept "application/json"
                                                                     :content-type "application/json"
                                                                     :query-params qps
                                                                     :throw-exceptions false}
                                                                    http-opts))
                                                         (parse-body "application/json")))
          submit-tx (fn [body] (-> (http/post (str *api-url* "/_crux/submit-tx")
                                              {:accept "application/json"
                                               :content-type "application/json"
                                               :as :stream
                                               :body (json/write-value-as-string {"tx-ops" body})
                                               :throw-exceptions false})
                                   (parse-body "application/json")))]
      (t/testing "/_crux/status"
        (t/is (= "crux.mem_kv.MemKv" (get (json-get {:url "/_crux/status"}) "kvStore"))))

      (let [{:strs [txId txTime] :as tx} (submit-tx [["put" {"_id" "test-person", "first-name" "George"}]])]
        (t/is (= 0 txId) "initial submit")
        (t/is (= {"txCommitted?" true}
                 (json-get {:url "/_crux/tx-committed?"
                            :qps {"txId" txId}})))
        (t/is (= tx
                 (json-get
                  {:url "/_crux/await-tx"
                   :qps {"txId" txId}})))
        (t/is (= {"txTime" txTime}
                 (json-get
                  {:url "/_crux/await-tx-time"
                   :qps {"txTime" txTime}})))
        (t/is (= {"txTime" txTime}
                 (json-get
                  {:url "/_crux/sync"})))
        (t/is (= tx
                 (json-get
                  {:url "/_crux/latest-completed-tx"})))
        (t/is (= {"txId" txId}
                 (json-get
                  {:url "/_crux/latest-submitted-tx"})))
        (t/testing "/_crux/entity"
          (t/is (= {"_id" "test-person", "first-name" "George"}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid" "test-person"}})
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid-json" (pr-str "test-person")}}))))
        (let [tx2 (submit-tx [["put" {"_id" "test-person", "first-name" "george"} "2020-09-30T20:05:50Z"]])
              tx3 (submit-tx [["put" {"_id" "test-person", "firstName" "George"}]])]
          (t/is (= 1 (get tx2 "txId")) "put with valid-time")
          (t/is (= 2 (get tx3 "txId")))
          (t/is (= tx3
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" 2}})))
          (t/testing "/_crux/entity?history=true"
            (let [entity-history (json-get
                                  {:url "/_crux/entity"
                                   :qps {"eid-json" (pr-str "test-person")
                                         "history" true
                                         "sort-order" "asc"
                                         "with-docs" true}})]
              (t/is (= 3 (count entity-history)))
              (t/is (= txTime (get-in entity-history [1 "txTime"])))
              (t/is (= {"_id" "test-person", "first-name" "george"} (get-in entity-history [0 "doc"])))
              (t/is (= {"_id" "test-person", "first-name" "George"} (get-in entity-history [1 "doc"])))
              (t/is (= {"_id" "test-person", "firstName" "George"} (get-in entity-history [2 "doc"])))))
          (t/testing "/_crux/tx-log"
            (let [tx-log (json-get
                          {:url "/_crux/tx-log"
                           :qps {"with-ops?" true}})]
              (t/is (= 3 (count tx-log)))
              (t/is (= (assoc tx "txOps" [["put" {"_id" "test-person", "first-name" "George"}]]) (get tx-log 0)))
              (t/is (= (assoc tx2 "txOps" [["put" {"_id" "test-person", "first-name" "george"} "2020-09-30T20:05:50Z"]]) (get tx-log 1)))
              (t/is (= (assoc tx3 "txOps" [["put" {"_id" "test-person", "firstName" "George"}]]) (get tx-log 2)))))))
      (t/testing "match operation"
        (let [{:strs [txId txTime] :as tx} (submit-tx [["match" "test-person" {"_id" "test-person", "firstName" "George"}]
                                                       ["put" {"_id" "test-person", "firstName" "George2"}]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= {"_id" "test-person", "firstName" "George2"}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid" "test-person"}})))))
      (t/testing "delete operation"
        (let [{:strs [txId] :as tx} (submit-tx [["delete" "test-person"]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= {"error" "test-person entity not found"}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid" "test-person"}})))))
      (t/testing "evict operation"
        (let [{:strs [txId] :as tx} (submit-tx [["evict" "test-person"]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= []
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid-json" (pr-str "test-person")
                           "history" true
                           "sort-order" "asc"
                           "with-docs" true}})))))
      (t/testing "/_crux/query"
        (let [{:strs [txId] :as tx} (submit-tx [["put" {"_id" "sal", "firstName" "Sally", "lastName" "Example"}]
                                                ["put" {"_id" "jed", "firstName" "Jed", "lastName" "Test"}]
                                                ["put" {"_id" "colin", "firstName" "Colin", "lastName" "Example"}]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))

          (t/is (= #{["sal"] ["jed"] ["colin"]}
                   (set
                    (json-get
                     {:url "/_crux/query"
                      :qps {"query" (pr-str '{:find [e]
                                              :where [[e :crux.db/id]]})}}))))
          (t/is (= (pr-str '{:find [e]
                             :where [[e :crux.db/id]]})
                   (get-in
                    (json-get
                     {:url "/_crux/recent-queries"})
                    [0 "query"])))
          (t/is (json-get
                 {:url "/_crux/query"
                  :qps {"query" (pr-str '{:find [e]
                                          :where [[e :crux.db/id]]})}}))
          (t/is (= #{["Sally"] ["Colin"]}
                   (set
                    (json-get
                     {:url "/_crux/query"
                      :qps {"query" (pr-str '{:find [first-name]
                                              :where [[e :firstName first-name]
                                                      [e :lastName "Example"]]})}}))))
          (t/is (= [[{"_id" "sal", "firstName" "Sally", "lastName" "Example"}]]
                   (json-get
                    {:url "/_crux/query"
                     :qps {"query" (pr-str '{:find [e]
                                             :where [[e :firstName "Sally"]]
                                             :full-results? true})}})))))
      (t/testing "transaction functions"
        (let [tx-fn (pr-str '(fn [ctx eid]
	                       (let [db (crux.api/db ctx)
	                             entity (crux.api/entity db eid)]
	                         [[:crux.tx/put (update entity :age inc)]])))
              {:strs [txId] :as tx} (submit-tx [["put" {"_id" "increment-age", "_fn" tx-fn}]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= {"_id" "increment-age", "_fn" tx-fn}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid-json" (pr-str "increment-age")}}))))
        (let [{:strs [txId] :as tx} (submit-tx [["put" {"_id" "ivan", "age" 21}]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= {"_id" "ivan" "age" 21}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid-json" (pr-str "ivan")}}))))
        (let [{:strs [txId] :as tx} (submit-tx [["fn" "increment-age" "ivan"]])]
          (t/is (= tx
                   (json-get
                    {:url "/_crux/await-tx"
                     :qps {"txId" txId}})))
          (t/is (= {"_id" "ivan" "age" 22}
                   (json-get
                    {:url "/_crux/entity"
                     :qps {"eid-json" (pr-str "ivan")}}))))))))
