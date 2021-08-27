(ns xtdb.http-server.ui-routes-test
  (:require [juxt.clojars-mirrors.clj-http.v3v12v2.clj-http.client :as http]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [cognitect.transit :as transit]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.http-server :as fh :refer [*api-url*]]
            [xtdb.http-server.entity-ref :as entity-ref]
            [juxt.clojars-mirrors.jsonista.v0v3v1.jsonista.core :as json]
            [xtdb.query-state :as cqs])
  (:import java.io.InputStream))

(t/use-fixtures :each
  fh/with-http-server
  fix/with-node)

(defn- parse-body [{:keys [^InputStream body]} content-type]
  (case content-type
    "application/transit+json" (transit/read (transit/reader body :json {:handlers {"xtdb.http/entity-ref" entity-ref/ref-read-handler
                                                                                    "xtdb/oid" (transit/read-handler c/id-edn-reader)
                                                                                    "xtdb/base64" (transit/read-handler c/base64-reader)}}))
    "application/json" (json/read-value body)
    "application/edn" (edn/read-string {:readers {'xtdb.http/entity-ref entity-ref/->EntityRef
                                                  'xtdb/query-state cqs/->QueryState
                                                  'xtdb/query-error cqs/->QueryError
                                                  'xtdb/id str}}
                                       (slurp body))
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
  (let [{::xt/keys [tx-id]} (-> (http/post (str *api-url* "/_xtdb/submit-tx")
                                           {:content-type :edn
                                            :body (pr-str {:tx-ops [[::xt/put {:xt/id :ivan, :linking :peter}]
                                                                    [::xt/put {:xt/id :peter, :name "Peter"}]]})
                                            :as :stream})
                                (parse-body "application/edn"))]
    (http/get (str *api-url* "/_xtdb/await-tx?tx-id=" tx-id))

    (t/testing "Test redirect on / endpoint."
      (t/is (= "/_xtdb/query" (-> (get-result-from-path "/")
                                  (get-in [:headers "Location"])))))

    (t/testing "getting the entity with different types"
      (let [get-entity (fn [accept-type] (-> (get-result-from-path "/_xtdb/entity?eid-edn=:peter" accept-type)
                                             (parse-body accept-type)))]
        (t/is (= {:xt/id :peter, :name "Peter"}
                 (get-entity "application/edn")))
        (t/is (= {:xt/id :peter, :name "Peter"}
                 (get-entity "application/transit+json")))))

    (t/testing "getting linked entities"
      (let [get-linked-entities (fn [accept-type]
                                  (-> (get-result-from-path "/_xtdb/entity?eid-edn=:ivan&link-entities?=true" accept-type)
                                      (parse-body accept-type)))]
        (t/is (= {:xt/id :ivan, :linking (entity-ref/->EntityRef :peter)}
                 (get-linked-entities "application/edn")))
        (t/is (= {:xt/id :ivan, :linking (entity-ref/->EntityRef :peter)}
                 (get-linked-entities "application/transit+json")))))

    (t/testing "getting query results (GET)"
      (let [get-query (fn [accept-type]
                        (set (-> (get-result-from-path (format "/_xtdb/query?query-edn=%s" '{:find [e] :where [[e :xt/id _]]}) accept-type)
                                 (parse-body accept-type))))]
        (t/is (= #{[:ivan] [:peter]} (get-query "application/edn")))
        (t/is (= #{[:ivan] [:peter]} (get-query "application/transit+json")))
        (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/csv")))
        (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/tsv")))))

    (t/testing "getting query results (POST)"
      (let [post-query (fn [{:keys [body content-type accept-type]}]
                         (let [accept-type (or accept-type content-type)
                               {:keys [status] :as results} (http/post (str *api-url* "/_xtdb/query")
                                                                       {:accept accept-type
                                                                        :content-type content-type
                                                                        :as :stream
                                                                        :body body
                                                                        :throw-exceptions false})]
                           (cond-> (parse-body results accept-type)
                             (= status 200) set)))]
        (t/is (= #{[:ivan] [:peter]} (post-query
                                      {:body (pr-str {:query '{:find [e] :where [[e :xt/id _]]}})
                                       :content-type "application/edn"})))
        (t/is (= #{[:ivan] [:peter]} (post-query
                                      {:body "[\"^ \",\"~:query\",[\"^ \",\"~:find\",[\"~$e\"],\"~:where\",[[\"~$e\",\"~:xt/id\",\"~$_\"]]]]"
                                       :content-type "application/transit+json"})))
        (t/is (= #{[":ivan"] [":peter"] ["e"]} (post-query
                                                {:body (pr-str {:query '{:find [e] :where [[e :xt/id _]]}})
                                                 :content-type "application/edn"
                                                 :accept-type "text/csv"})))
        (t/is (= #{[":ivan"] [":peter"] ["e"]} (post-query
                                                {:body (pr-str {:query '{:find [e] :where [[e :xt/id _]]}})
                                                 :content-type "application/edn"
                                                 :accept-type "text/tsv"})))

        (t/testing "POSTing malformed queries"
          (t/is (= {:error "Malformed \"application/edn\" request."}
                   (post-query
                    {:body "{:query {:find [e] :where [[e :xt/id _]}"
                     :content-type "application/edn"})))

          (t/is (= {:error "Malformed \"application/transit+json\" request."}
                   (post-query
                    {:body "[\"^ \",\"~:query\",[\"^ \",\"~:find\",[\"~$e\"],\"~:where\",[[\"~$e\",\"~:xt/id\",\"~$_\"]]]"
                     :content-type "application/transit+json"})))))))

  (t/testing "getting linked entities in query results"
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path (format "/_xtdb/query?query-edn=%s&link-entities?=true" '{:find [e] :where [[e :xt/id _]]}) accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/edn")))
      (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/transit+json")))))

  (t/testing "file-type based negotiation"
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path (format "/_xtdb/query.csv?query-edn=%s" '{:find [e] :where [[e :xt/id _]]}))
                      (parse-body "text/csv")))))
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path (format "/_xtdb/query.tsv?query-edn=%s" '{:find [e] :where [[e :xt/id _]]}))
                      (parse-body "text/tsv")))))))

(t/deftest test-string-eid-routes
  (let [{::xt/keys [tx-id] :as tx} (fix/submit+await-tx *api* [[::xt/put {:xt/id "string-id"}]])]
    (t/is (= {:xt/id "string-id"}
             (-> (get-result-from-path "/_xtdb/entity?eid=string-id" "application/edn")
                 (parse-body "application/edn"))))
    (t/is (= tx-id
             (-> (get-result-from-path "/_xtdb/entity-tx?eid=string-id" "application/edn")
                 (parse-body "application/edn")
                 ::xt/tx-id)))))

(t/deftest test-b64
  (fix/submit+await-tx *api* [[::xt/put {:xt/id :foo, :bytes (byte-array [1 2 3])}]])
  (t/is (= {:xt/id :foo
            :bytes [1 2 3]}
           (-> (get-result-from-path "/_xtdb/entity?eid-edn=:foo" "application/transit+json")
               (parse-body "application/transit+json")
               (update :bytes seq)))))
