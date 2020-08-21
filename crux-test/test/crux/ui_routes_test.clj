(ns crux.ui-routes-test
  (:require [crux.fixtures :as fix]
            [clojure.edn :as edn]
            [clojure.test :as t]
            [crux.api :as crux]
            [crux.codec :as c]
            [clj-http.client :as http]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [crux.http-server.entity-ref :as entity-ref]
            [clojure.java.io :as io])
  (:import (java.io InputStream)))

(t/use-fixtures :each
  fix/with-standalone-topology
  fh/with-http-server
  fix/with-node)

(defn- parse-body [{:keys [^InputStream body]} content-type]
  (case content-type
    "application/transit+json" (transit/read (transit/reader body :json {:handlers {"crux.http/entity-ref" entity-ref/ref-read-handler}}))
    "application/edn" (edn/read-string {:readers {'crux.http/entity-ref entity-ref/->EntityRef}} (slurp body))
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
                                                                       :body (pr-str '[[:crux.tx/put {:crux.db/id :ivan, :linking :peter}]
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
                                    (parse-body accept-type)))]
      (t/is (= {:crux.db/id :ivan, :linking (entity-ref/->EntityRef :peter)}
               (get-linked-entities "application/edn"))))
      ; (t/is (= {:crux.db/id :ivan, :linking (entity-ref/->EntityRef :peter)}
      ;          (get-linked-entities "application/transit+json"))))

    ;; Testing getting query results
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path "/_crux/query?find=[e]&where=[e+%3Acrux.db%2Fid+_]" accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[:ivan] [:peter]} (get-query "application/edn")))
      (t/is (= #{[:ivan] [:peter]} (get-query "application/transit+json")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/csv")))
      (t/is (= #{[":ivan"] [":peter"] ["e"]} (get-query "text/tsv"))))

    ;; Testing getting linked entities in query results
    (let [get-query (fn [accept-type]
                      (set (-> (get-result-from-path "/_crux/query?find=[e]&where=[e+%3Acrux.db%2Fid+_]&link-entities?=true" accept-type)
                               (parse-body accept-type))))]
      (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/edn")))
      (t/is (= #{[(entity-ref/->EntityRef :ivan)] [(entity-ref/->EntityRef :peter)]} (get-query "application/transit+json"))))

    ;; Test file-type based negotiation
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path "/_crux/query.csv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/csv")))))
    (t/is (= #{[":ivan"] [":peter"] ["e"]}
             (set (-> (get-result-from-path "/_crux/query.tsv?find=[e]&where=[e+%3Acrux.db%2Fid+_]")
                      (parse-body "text/tsv")))))))

(t/deftest test-history-pagination
  (letfn [(submit-ivan [valid-time version]
            (-> (http/post (str *api-url* "/tx-log")
                           {:content-type :edn
                            :body (pr-str [[:crux.tx/put {:crux.db/id :ivan, :crux.db/valid-time valid-time, :name "Ivan" :version version}]])
                            :as :stream})
              (parse-body "application/edn")))

          (create-ivan-history [n]
            (let [valid-times [#inst "2020-08-19" #inst "2020-08-20" #inst "2020-08-21"]]
              (doseq [i (range 1 (inc n))]
                (let [{:keys [crux.tx/tx-id]} (submit-ivan (rand-nth valid-times) i)]
                  (http/get (str *api-url* "/await-tx?tx-id=" tx-id))))))]

      (create-ivan-history 20)

      ;; Test getting the entity history with different types and limits
      (let [get-entity-history (fn [accept-type limit] (-> (get-result-from-path (str "/_crux/entity?eid=:ivan&history=true&sort-order=desc&with-docs=true" (when-not (zero? limit) (str "&limit=" limit)))  accept-type)
                                                         (parse-body accept-type)))]
        (doseq [h (get-entity-history "application/edn" 10)]
          (println h))

        (t/is (= 10 (count (get-entity-history "application/edn" 10))))
        (t/is (= 10 (count (get-entity-history "application/transit+json" 10))))
        (t/is (= 20 (count (get-entity-history "application/edn" 0)))))))
