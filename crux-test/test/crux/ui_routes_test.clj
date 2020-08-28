(ns crux.ui-routes-test
  (:require [crux.fixtures :as fix]
            [clojure.edn :as edn]
            [clojure.test :as t]
            [crux.api :as crux]
            [clj-http.client :as http]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.http-server.entity :as entity]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import  java.io.InputStream
            java.util.Date
            [java.net URL URLDecoder]
            [java.time Instant ZonedDateTime ZoneId]))

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
                 (doall (csv/read-csv rdr :separator \tab)))
    :not-found))

(defn- parse-history-continuation-link [res]
  (let [qry (some->> (get-in res [:links :next :href])
              (URL.)
              (.getQuery)
              (URLDecoder/decode))]
    (when qry
      (->> (str/split qry #"&")
        (map #(str/split % #"="))
        (map #(vector (keyword (first %)) (last %)))
        (into {})))))

(defn- relative-path [url]
  (->> (URL. url)
    ((juxt #(.getPath %) #(.getQuery %)))
    (str/join "?")))

(defn- get-result-from-path
  ([path]
   (get-result-from-path path "application/edn"))
  ([path accept-type]
   (http/get (str *api-url* path)
             {:accept accept-type
              :as :stream})))

(defn- normalize-date
  [^Date t]
  (some->> t
    (.toInstant)
    ^ZonedDateTime ((fn [^Instant inst] (.atZone inst (ZoneId/of "Z"))))
    (.format entity/iso-format)))

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
               (get-linked-entities "application/edn")))
      (t/is (= {:crux.db/id :ivan, :linking (entity-ref/->EntityRef :peter)}
               (get-linked-entities "application/transit+json"))))

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
  (letfn [(submit-ivan [version valid-time]
            (-> (http/post (str *api-url* "/tx-log")
                           {:content-type :edn
                            :body (pr-str [[:crux.tx/put {:crux.db/id :ivan, :name "Ivan" :version version} valid-time]])
                            :as :stream})
              (parse-body "application/edn")))

          ;; Create n items of history, in batches of m at the same valid-time
          (create-ivan-history [start-id n m]
            (let [valid-times [#inst "2020-08-24T12:00:00"
                               #inst "2020-08-25T12:00:00"
                               #inst "2020-08-26T12:00:00"
                               #inst "2020-08-27T12:00:00"]
                  ids (partition-all m (range start-id (+ start-id n)))
                  batches (zipmap ids (cycle valid-times))]
              (doseq [[versions valid-time] batches
                      version versions]
                (submit-ivan  version valid-time))
              (http/get (str *api-url* "/await-tx?tx-id=" (+ start-id (dec n))))))]

      (let [res (create-ivan-history 0 20 3)
            tx-time (normalize-date (:crux.tx/tx-time (read-string (:body res))))
            path "/_crux/entity?eid=:ivan&history=true&sort-order=asc&with-docs=true&with-corrections=true"
            parse-response (fn [res accept-type] {:body (parse-body res accept-type)
                                                  :link (get-in res [:links :next :href])
                                                  :link-params (parse-history-continuation-link res)})
            get-entity-history (fn [path accept-type limit] (-> (get-result-from-path (str path (when-not (zero? limit) (str "&limit=" limit)))  accept-type)
                                                              (parse-response accept-type)))]

        ;; First 10 items of history
        (let [hist (get-entity-history path "application/edn" 10)]
          (t/is (= 10 (count (:body hist))))
          (t/is (= tx-time (get-in hist [:link-params :transaction-time])))
          (t/is (= [0 1 2 12 13 14 3 4 5 15] (map :crux.tx/tx-id (:body hist))))

          ; Add some more history, to check that the next page doesn't include these items
          (create-ivan-history 20 5 3)

          ;; Next page should be the last, with only 10 items
          (let [nxt (get-entity-history (relative-path (:link hist)) "application/edn" 0)]
            (println (count (:body nxt)))
            (t/is (= 10 (count (:body nxt))))
            (t/is (= nil (:link nxt)))
            (t/is (= [16 17 6 7 8 18 19 9 10 11] (map :crux.tx/tx-id (:body nxt))))))

        ;; First 10 items of history, as at the original transaction time
        (let [hist (get-entity-history (str path "&transaction-time=" tx-time) "application/transit+json" 10)
              nxt (get-entity-history (relative-path (:link hist)) "application/transit+json" 0)]
          (t/is (= 10 (count (:body hist))))
          (t/is (= tx-time (get-in hist [:link-params :transaction-time])))
          (t/is (= [0 1 2 12 13 14 3 4 5 15] (map :crux.tx/tx-id (:body hist))))
          (t/is (= 10 (count (:body nxt))))
          (t/is (= [16 17 6 7 8 18 19 9 10 11] (map :crux.tx/tx-id (:body nxt))))
          (t/is (= nil (:link nxt))))

        ;; Unrestricted history - should now return 25 items
        (let [hist (get-entity-history path "application/edn" 0)]
          (t/is (= 25 (count (:body hist))))
          (t/is (= [0 1 2 12 13 14 20 21 22 3 4 5 15 16 17 23 24 6 7 8 18 19 9 10 11]
                   (map :crux.tx/tx-id (:body hist))))
          (t/is (= nil (:link hist))))

        ;; Unrestricted history - should now return 25 items
        (let [hist (get-entity-history path "application/transit+json" 0)]
          (t/is (= 25 (count (:body hist))))
          (t/is (= [0 1 2 12 13 14 20 21 22 3 4 5 15 16 17 23 24 6 7 8 18 19 9 10 11]
                   (map :crux.tx/tx-id (:body hist))))
          (t/is (= nil (:link hist)))))))
