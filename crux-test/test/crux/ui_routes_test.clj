(ns crux.ui-routes-test
  (:require [crux.fixtures :as fix]
            [clojure.test :as t]
            [crux.api :as crux]
            [clj-http.client :as http]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(t/use-fixtures :each
  fix/with-standalone-topology
  fh/with-http-server
  fix/with-node)

(defn- parse-body [{:keys [^InputStream body]} content-type]
  (case content-type
    "application/transit+json" (transit/read (transit/reader body :json))
    "application/edn" (read-string (slurp body))
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
              :as :stream})))

(t/deftest test-ui-routes
  ;; Insert data
  (let [{:keys [crux.tx/tx-id crux.tx/tx-time] :as tx} (-> (http/post (str *api-url* "/tx-log")
                                                                      {:content-type :edn
                                                                       :body (pr-str '[[:crux.tx/put {:crux.db/id :ivan, :linking :peter, :link2 :petr}]
                                                                                       [:crux.tx/put {:crux.db/id :peter, :name "Peter"}]])
                                                                       :as :stream})
                                                           (parse-body "application/edn"))]
    (http/get (str *api-url* "/await-tx?tx-id=" tx-id))

    ;; Test redirect on "/" endpoint.
    (t/is (= "/_crux/index.html" (-> (get-result-from-path "/")
                                     (get-in [:headers "Content-Location"]))))

    ;; Test getting the entity with different types
    (let [get-entity (fn [accept-type] (-> (get-result-from-path "/_crux/entity?eid=:peter" accept-type)
                                           (parse-body accept-type)))]
      (t/is (= {:crux.db/id :peter, :name "Peter"}
               (get-entity "application/edn")))
      (t/is (= {:crux.db/id :peter, :name "Peter"}
               (get-entity "application/transit+json"))))

    ;; Test getting linked entities
    (let [get-linked-entities (fn [accept-type]
                                (-> (get-result-from-path "/_crux/entity?eid=:ivan&link-entities?=true" accept-type)
                                    (parse-body accept-type)
                                    (get :entity-links)))]
      (t/is (= #{:ivan :peter}
               (get-linked-entities "application/edn")))
      (t/is (= #{:ivan :peter}
               (get-linked-entities "application/transit+json"))))

    ;; Testing getting query results
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path "/_crux/query?find=[e]&where=[e+%3Acrux.db%2Fid+_]" accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[:ivan] [:peter]} (get-query "application/edn")))
      (t/is (= #{[:ivan] [:peter]} (get-query "application/transit+json")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/csv")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/tsv"))))

    ;; Test file-type based negotiation
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path "/_crux/query.csv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/csv")))))
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path "/_crux/query.tsv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/tsv")))))))
