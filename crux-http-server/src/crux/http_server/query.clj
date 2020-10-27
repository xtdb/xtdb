(ns crux.http-server.query
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [crux.api :as crux]
            [crux.codec :as c]
            [crux.error :as err]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.http-server.json :as http-json]
            [crux.http-server.util :as util]
            [crux.io :as cio]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [muuntaja.format.edn :as mfe]
            [muuntaja.format.transit :as mft]
            [spec-tools.core :as st])
  (:import [java.io Closeable OutputStream]
           [java.time Instant ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date))

(s/def ::query
  (st/spec
   {:spec any? ; checked by crux.query
    :swagger/example '{:find [e] :where [[e :crux.db/id _]] :limit 100}
    :description "Datalog query"}))

(s/def ::query-edn
  (st/spec
   {:spec ::query
    :swagger/example (pr-str '{:find [e] :where [[e :crux.db/id _]] :limit 100})
    :description "EDN formatted Datalog query"
    :decode/string (fn [_ q] (util/try-decode-edn q))}))

;; TODO: Need to ensure all query clauses are present + coerced properly
(s/def ::query-params
  (s/keys :opt-un [::util/valid-time ::util/transact-time ::util/link-entities? ::query-edn]))

(s/def ::args (s/coll-of any? :kind vector?))

(s/def ::body-params
  (s/keys :req-un [::query]
          :opt-un [::args]))

(def query-root-str
  (string/join "\n"
               [";; Welcome to the Crux Console!"
                ";; To perform a query:"
                ";; 1) Enter a query into this query editor, such as the following example"
                ";; 2) Optionally, select a \"valid time\" and/or \"transaction time\" to query against"
                ";; 3) Submit the query and the tuple results will be displayed in a table below"
                ""
                "{"
                " :find [?e]                ;; return a set of tuples each consisting of a unique ?e value"
                " :where [[?e :crux.db/id]] ;; select ?e as the entity id for all entities in the database"
                "}"]))

(defn with-entity-refs
  [results db]
  (let [entity-links (->> (apply concat results)
                          (into #{} (filter c/valid-id?))
                          (into #{} (filter #(crux/entity db %))))]
    (->> results
         (map (fn [tuple]
                (->> tuple
                     (mapv (fn [el]
                             (cond-> el
                               (get entity-links el) (entity-ref/->EntityRef))))))))))

(defn run-query [{:keys [link-entities? query]} {:keys [crux-node valid-time transaction-time]}]
  (let [db (util/db-for-request crux-node {:valid-time valid-time
                                           :transact-time transaction-time})]
    {:query query
     :valid-time (crux/valid-time db)
     :transaction-time (crux/transaction-time db)
     :results (if link-entities?
                (let [results (crux/q db query)]
                  (cio/->cursor (fn []) (with-entity-refs results db)))
                (crux/open-q db query))}))

(defn- ->*sv-encoder [{:keys [sep]}]
  (reify mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [results query] :as res} charset]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (try
            (if results
              (csv/write-csv w (cons (:find query) (iterator-seq results)) :separator sep)
              (.write w (pr-str res)))
            (finally
              (cio/try-close results))))))))

(defn ->html-encoder [{:keys [crux-node http-options]}]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [no-query? cause ^Closeable results] :as res} charset]
      (try
        (let [^String resp (util/raw-html {:title "/_crux/query"
                                           :crux-node crux-node
                                           :http-options http-options
                                           :results (cond
                                                      no-query? nil
                                                      results (try
                                                              {:query-results (iterator-seq results)}
                                                              (finally
                                                                (.close results)))
                                                      :else {:query-results
                                                             {"error" res}})})]
          (.getBytes resp ^String charset))
        (finally
          (cio/try-close results))))))

(defn ->query-muuntaja [opts]
  (m/create (-> m/default-options
                (dissoc :formats)
                (assoc :return :output-stream
                       :default-format "application/edn")
                (m/install {:name "text/csv"
                            :encoder [->*sv-encoder {:sep \,}]})
                (m/install {:name "text/tsv"
                            :encoder [->*sv-encoder {:sep \tab}]})
                (m/install {:name "text/html"
                            :encoder [->html-encoder opts]
                            :return :bytes})
                (m/install {:name "application/edn"
                            :encoder [util/->edn-encoder]
                            :decoder [mfe/decoder]})
                (m/install {:name "application/transit+json"
                            :encoder [util/->tj-encoder]
                            :decoder [(partial mft/decoder :json)]})
                (m/install {:name "application/json"
                            :encoder [http-json/->json-encoder]}))))

(defmulti transform-req
  (fn [query req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-req "text/html" [query req]
  {:query (-> query
              (dissoc :full-results))
   :link-entities? true})

(defmethod transform-req "text/csv" [query req]
  {:query (-> query
              (dissoc :full-results))})

(defmethod transform-req "text/tsv" [query req]
  {:query (-> query
              (dissoc :full-results))})

(defmethod transform-req :default [query req]
  {:query query
   :link-entities? (get-in req [:parameters :query :link-entities?])})

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(def ^DateTimeFormatter csv-date-formatter
  (-> (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmssXXX")
      (.withZone (ZoneId/of "Z"))))

(defn with-download-header [resp {:keys [results transaction-time]} ext]
  (-> resp
      (assoc-in [:headers "Content-Disposition"]
                (format "attachment; filename=query-%s.%s"
                        (.format csv-date-formatter ^Instant (.toInstant ^Date transaction-time))
                        ext))))

(defmethod transform-query-resp "text/csv" [{:keys [no-query?] :as res} _]
  (cond
    no-query? (throw (err/illegal-arg :no-query {::err/message "No query provided"}))
    :else (-> {:status 200, :body res}
              (with-download-header res "csv"))))

(defmethod transform-query-resp "text/tsv" [{:keys [no-query?] :as res} _]
  (cond
    no-query? (throw (err/illegal-arg :no-query {::err/message "No query provided"}))
    :else (-> {:status 200, :body res}
              (with-download-header res "tsv"))))

(defmethod transform-query-resp "text/html" [res _]
  {:status 200 :body res})

(defmethod transform-query-resp :default [{:keys [no-query?] :as res} _]
  (cond
    no-query? (throw (err/illegal-arg :no-query {::err/message "No query provided"}))
    :else {:status 200, :body res}))

(defn data-browser-query [options]
  (fn [req]
    (let [{query-params :query body-params :body} (get-in req [:parameters])
          {:keys [valid-time transaction-time query-edn]} query-params
          query (or query-edn (get body-params :query))]
      (-> (if (nil? query)
            (assoc options :no-query? true)
            (run-query (transform-req query req)
                       (assoc options
                              :valid-time valid-time
                              :transaction-time transaction-time)))
          (transform-query-resp req)))))
