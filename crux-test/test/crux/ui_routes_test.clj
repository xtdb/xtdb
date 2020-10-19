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
                      (set (-> (get-result-from-path (format "/_crux/query?query-edn=%s" '{:find [e] :where [[e :crux.db/id _]]}) accept-type)
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
                    (set (-> (get-result-from-path (format "/_crux/query?query-edn=%s&link-entities?=true" '{:find [e] :where [[e :crux.db/id _]]}) accept-type)
                             (parse-body accept-type))))]
    (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/edn")))
    (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/transit+json"))))

  ;; Test file-type based negotiation
  (t/is (= #{[":ivan"] [":peter"] ["e"]}
           (set (-> (get-result-from-path (format "/_crux/query.csv?query-edn=%s" '{:find [e] :where [[e :crux.db/id _]]}))
                    (parse-body "text/csv")))))
  (t/is (= #{[":ivan"] [":peter"] ["e"]}
           (set (-> (get-result-from-path (format "/_crux/query.tsv?query-edn=%s" '{:find [e] :where [[e :crux.db/id _]]}))
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
