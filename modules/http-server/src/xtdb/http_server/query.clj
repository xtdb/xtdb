(ns xtdb.http-server.query
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.error :as err]
            [xtdb.http-server.entity-ref :as entity-ref]
            [xtdb.http-server.json :as http-json]
            [xtdb.http-server.util :as util]
            [xtdb.io :as xio]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.core :as m]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.edn :as mfe]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.transit :as mft]
            [juxt.clojars-mirrors.spec-tools.v0v10v5.spec-tools.core :as st])
  (:import [java.io Closeable OutputStream]
           [java.time Instant ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date))

(s/def ::query
  (st/spec
   {:spec any? ; checked by xtdb.query
    :swagger/example '{:find [e] :where [[e :xt/id _]] :limit 100}
    :description "Datalog query"}))

(s/def ::query-edn
  (st/spec
   {:spec ::query
    :swagger/example (pr-str '{:find [e] :where [[e :xt/id _]] :limit 100})
    :description "EDN formatted Datalog query"
    :decode/string (fn [_ q] (util/try-decode-edn q))}))

(s/def ::in-args
  (st/spec
   {:spec (s/coll-of any? :kind vector?)
    :swagger/example ["foo" 123]
    :description ":in binding arguments"}))

(s/def ::in-args-edn
  (st/spec
   {:spec ::in-args
    :swagger/example (pr-str '["foo" 123])
    :description "EDN formatted :in binding arguments"
    :decode/string (fn [_ a] (util/try-decode-edn a))}))

(s/def ::in-args-json
  (st/spec
   {:spec ::in-args
    :swagger/example (http-json/write-str '["foo" 123])
    :description "JSON formatted :in binding arguments"
    :decode/string (fn [_ a] (http-json/try-decode-json a))}))

;; TODO: Need to ensure all query clauses are present + coerced properly
(s/def ::query-params
  (s/keys :opt-un [::util/valid-time ::util/tx-time ::util/tx-id ::util/link-entities?
                   ::query-edn ::in-args-edn ::in-args-json]))

(s/def ::body-params
  (s/keys :req-un [::query]
          :opt-un [::in-args]))

(defn with-entity-refs
  [results db]
  (let [entity-links (->> (apply concat results)
                          (into #{} (filter c/valid-id?))
                          (into #{} (filter #(xt/entity db %))))]
    (->> results
         (map (fn [tuple]
                (->> tuple
                     (mapv (fn [el]
                             (cond-> el
                               (get entity-links el) (entity-ref/->EntityRef))))))))))

(defn run-query [{:keys [link-entities? query]} in-args {:keys [xtdb-node valid-time tx-time tx-id]}]
  (let [db (util/db-for-request xtdb-node {:valid-time valid-time
                                           :tx-time tx-time
                                           :tx-id tx-id})
        basis (xt/db-basis db)]
    {:query query
     :valid-time (get-in basis [::xt/tx ::xt/valid-time])
     :tx-time (get-in basis [::xt/tx ::xt/tx-time])
     :tx-id (get-in basis [::xt/tx ::xt/tx-id])
     :results (if link-entities?
                (let [results (apply xt/q db query in-args)]
                  (xio/->cursor (fn []) (with-entity-refs results db)))
                (apply xt/open-q db query in-args))}))

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
              (xio/try-close results))))))))

(defn ->html-encoder [{:keys [xtdb-node http-options]}]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [no-query? cause ^Closeable results] :as res} charset]
      (try
        (let [^String resp (util/raw-html {:title "/_xtdb/query"
                                           :xtdb-node xtdb-node
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
          (xio/try-close results))))))

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
  {:query query
   :link-entities? true})

(defmethod transform-req "text/csv" [query req]
  {:query query})

(defmethod transform-req "text/tsv" [query req]
  {:query query})

(defmethod transform-req :default [query req]
  {:query query
   :link-entities? (get-in req [:parameters :query :link-entities?])})

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(def ^DateTimeFormatter csv-date-formatter
  (-> (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmssXXX")
      (.withZone (ZoneId/of "Z"))))

(defn with-download-header [resp {:keys [results tx-time]} ext]
  (-> resp
      (assoc-in [:headers "Content-Disposition"]
                (format "attachment; filename=query-%s.%s"
                        (.format csv-date-formatter ^Instant (.toInstant ^Date tx-time))
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
    (try
      (let [{query-params :query body-params :body} (get-in req [:parameters])
            {:keys [valid-time tx-time tx-id query-edn in-args-edn in-args-json]} query-params
            query (or query-edn (get body-params :query))
            in-args (or in-args-edn in-args-json (get body-params :in-args))]
        (-> (if (nil? query)
              (assoc options :no-query? true)
              (run-query (transform-req query req)
                         in-args
                         (assoc options
                                :valid-time valid-time
                                :tx-time tx-time
                                :tx-id tx-id)))
            (transform-query-resp req)))

      (catch java.lang.AssertionError e
        (log/debug e)
        (throw e)))))
