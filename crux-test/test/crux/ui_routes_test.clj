(ns crux.ui-routes-test
  (:require [crux.fixtures :as fix]
            [clojure.test :as t]
            [crux.api :as crux]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]))

(t/use-fixtures :each
  fix/with-standalone-topology
  fh/with-http-server
  fix/with-node)

(defn- parse-body [response content-type]
  (let [body-str (:body response)]
    (case content-type
      "application/edn" (read-string body-str)
      "application/json" (json/read-str body-str)
      "text/csv" (csv/read-csv body-str)
      "text/tsv" (csv/read-csv body-str :separator \tab))))

(defn- get-result-from-path
  ([path]
   (get-result-from-path path "application/edn"))
  ([path accept-type]
   (http/get
    (str *api-url* path)
    {:accept accept-type})))

(t/deftest test-ui-routes
  ;; Insert data
  (let [{:keys [crux.tx/tx-id crux.tx/tx-time] :as tx} (-> (http/post (str *api-url* "/tx-log")
                                                                      {:content-type :edn
                                                                       :body (pr-str '[[:crux.tx/put {:crux.db/id :ivan, :linking :peter}]
                                                                                       [:crux.tx/put {:crux.db/id :peter, :name "Peter"}]])})
                                                           (parse-body "application/edn"))]
    (http/get (str *api-url* "/await-tx?tx-id=" tx-id))

    ;; Test redirect on "/" endpoint.
    (t/is (= "/_crux/index.html" (-> (get-result-from-path "/")
                                     (get-in [:headers "Content-Location"]))))

    ;; Test getting the entity with different types
    (let [get-entity (fn [accept-type] (-> (get-result-from-path "/_crux/entity?eid=:peter" accept-type)
                                           (parse-body accept-type)))]
      (t/is (= {:crux.db/id :peter, :name "Peter"} (get-entity "application/edn")))
      (t/is (= {"crux.db/id" "peter", "name" "Peter"} (get-entity "application/json"))))

    ;; Test getting linked entities
    (let [get-linked-entities (fn [accept-type] (-> (get-result-from-path "/_crux/entity?eid=:ivan&link-entities?=true" accept-type)
                                                    (parse-body accept-type)
                                                    (get "linked-entities")))]
      (t/is (:ivan (get-linked-entities "application/edn")))
      (t/is (get (get-linked-entities "application/json") "ivan")))

    ;; Testing getting query results
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path "/_crux/query?find=[e]&where=[e+%3Acrux.db%2Fid+_]" accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[:ivan] [:peter]} (get-query "application/edn")))
      (t/is (= #{["ivan"] ["peter"]} (get-query "application/json")))
      (t/is (= #{[":ivan"] [":peter"]} (get-query "text/csv")))
      (t/is (= #{[":ivan"] [":peter"]} (get-query "text/tsv"))))

    ;; Test file-type based negotiation
    (t/is (= #{[":ivan"] [":peter"]}
             (set (-> (get-result-from-path "/_crux/query.csv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/csv")))))
    (t/is (= #{[":ivan"] [":peter"]}
             (set (-> (get-result-from-path "/_crux/query.tsv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/tsv")))))))
