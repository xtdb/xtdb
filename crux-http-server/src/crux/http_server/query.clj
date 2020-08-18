(ns crux.http-server.query
  (:require [crux.http-server.util :as util]
            [crux.http-server.entity-ref :as entity-ref]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.instant :as instant]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.api :as api]
            [crux.codec :as c]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [crux.io :as cio]
            [crux.db :as db])
  (:import (java.io OutputStream Writer)
           [java.time Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date
           crux.http_server.entity_ref.EntityRef))

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

(defn- query-root-html []
  [:div.query-root
   [:h1.query-root__title
    "Query"]
   [:div.query-root__contents
    [:p "Enter a "
     [:a {:href "https://opencrux.com/reference/queries.html#basic-query" :target "_blank"} "Datalog"]
     " query below to retrieve a set of facts from your database. Datalog queries must contain a `:find` key and a `:where` key."]
    [:div.query-editor__title
      "Datalog query editor"]
    [:div.query-editor__contents
     [:form
      {:action "/_crux/query"}
      [:textarea.textarea
       {:name "q"
        :rows 10
        :cols 40}
       query-root-str]
      [:div.query-editor-datetime
       [:div.query-editor-datetime-input
        [:b "Valid Time"]
        [:input.input.input-time
         {:type "datetime-local"
          :name "valid-time"
          :step "0.01"
          :value (.format util/default-date-formatter (ZonedDateTime/now (ZoneId/of "Z")))}]]
       [:div.query-editor-datetime-input
        [:b "Transaction Time"]
        [:input.input.input-time
         {:type "datetime-local"
          :name "transaction-time"
          :step "0.01"}]]]
      [:button.button
       {:type "submit"}
       "Submit Query"]]]]])

(defn- vectorize-param [param]
  (if (vector? param) param [param]))

(defn- build-query [{:strs [find where args order-by limit offset full-results link-entities?]}]
  (let [new-offset (if offset
                     (Integer/parseInt offset)
                     0)]
    (cond-> {:find (c/read-edn-string-with-readers find)
             :where (->> where vectorize-param (mapv c/read-edn-string-with-readers))
             :offset new-offset}
      args (assoc :args (->> args vectorize-param (mapv c/read-edn-string-with-readers)))
      order-by (assoc :order-by (->> order-by vectorize-param (mapv c/read-edn-string-with-readers)))
      limit (assoc :limit (Integer/parseInt limit))
      full-results (assoc :full-results? true)
      link-entities? (assoc :link-entities? true))))

(defn with-entity-refs
  [results db]
  (let [entity-links (->> (apply concat results)
                          (into #{} (filter c/valid-id?))
                          (into #{} (filter #(api/entity db %))))]
    (->> results
         (map (fn [tuple]
                (->> tuple
                     (mapv (fn [el]
                             (cond-> el
                               (get entity-links el) (entity-ref/->EntityRef))))))))))



(defn resolve-prev-next-offset
  [query-params prev-offset next-offset]
  (let [url (str "/_crux/query?"
                 (subs
                  (->> (dissoc query-params "offset")
                       (reduce-kv (fn [coll k v]
                                    (if (vector? v)
                                      (apply str coll (mapv #(str "&" k "=" %) v))
                                      (str coll "&" k "=" v))) ""))
                  1))
        prev-url (when prev-offset (str url "&offset=" prev-offset))
        next-url (when next-offset (str url "&offset=" next-offset))]
    {:prev-url prev-url
     :next-url next-url}))

(defn query->html [{:keys [results query] :as res}]
  (let [headers (:find query)]
    [:body
     [:div.uikit-table
      [:div.table__main
       [:table.table
        [:thead.table__head
         [:tr
          (for [header headers]
            [:th.table__cell.head__cell.no-js-head__cell
             header])]]
        (if (seq results)
          [:tbody.table__body
           (for [row results]
             [:tr.table__row.body__row
              (for [[header cell-value] (map vector headers row)]
                [:td.table__cell.body__cell
                 (if (instance? EntityRef cell-value)
                   [:a {:href (entity-ref/EntityRef->url cell-value res)} (str (:eid cell-value))]
                   (str cell-value))])])]
          [:tbody.table__body.table__no-data
           [:tr [:td.td__no-data
                 "Nothing to show"]]])]]
      [:table.table__foot]]]))

(defn run-query [{:keys [link-entities?] :as query} {:keys [crux-node valid-time transaction-time]}]
  (try
    (let [db (util/db-for-request crux-node {:valid-time valid-time
                                             :transact-time transaction-time})]
      {:query query
       :valid-time (api/valid-time db)
       :transaction-time (api/transaction-time db)
       :results (if link-entities?
                  (let [results (api/q db query)]
                    (cio/->cursor (fn []) (with-entity-refs results db)))
                  (api/open-q db query))})
    (catch Exception e
      {:error e})))

(defn- ->*sv-encoder [{:keys [sep]}]
  (reify mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [results query error]} charset]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (try
            (if error
              (.write w ^String error)
              (csv/write-csv w (cons (:find query) (iterator-seq results)) :separator sep))
            (finally
              (cio/try-close results))))))))

(defn ->html-encoder [opts]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [no-query? error results] :as res} charset]
      (try
        (let [^String resp (cond
                             no-query? (util/raw-html {:body (query-root-html)
                                                       :title "/query"
                                                       :options opts})
                             error (let [error-message (.getMessage ^Exception error)]
                                     (util/raw-html {:title "/query"
                                                     :body [:div.error-box error-message]
                                                     :options opts
                                                     :results {:query-results
                                                               {"error" error-message}}}))
                             :else (let [results (iterator-seq results)]
                                     (util/raw-html {:body (query->html (assoc res :results (drop-last results)))
                                                     :title "/query"
                                                     :options opts
                                                     :results {:query-results results}})))]
          (.getBytes resp ^String charset))
        (finally
          (cio/try-close results))))))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [results error] :as res} _]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (try
            (if error
              (.write w ^String (pr-str res))
              (print-method (iterator-seq results) w))
            (finally
              (cio/try-close results))))))))

(defn- ->tj-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [results error] :as res} _]
      (fn [^OutputStream output-stream]
        (try
          (if error
            (transit/write (transit/writer output-stream :json) res)
            (transit/write (transit/writer output-stream :json {:handlers {EntityRef entity-ref/ref-write-handler}}) (iterator-seq results)))
          (finally
            (cio/try-close results)))))))

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
                            :encoder [->edn-encoder]})
                (m/install {:name "application/transit+json"
                            :encoder [->tj-encoder]}))))

(defmulti transform-query-req
  (fn [query req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-req "text/html" [query req]
  (-> query
      (dissoc :full-results)
      (update :limit #(if % (inc %) 101))
      (assoc :link-entities? true)))

(defmethod transform-query-req "text/csv" [query req]
  (-> query
      (dissoc :full-results)
      (dissoc :link-entities?)))

(defmethod transform-query-req "text/tsv" [query req]
  (-> query
      (dissoc :full-results)
      (dissoc :link-entities?)))

(defmethod transform-query-req :default [query _] query)

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

(defn handle-error [{:keys [no-query? error]}]
  (cond
    no-query? {:status 400, :body "No query provided."}
    error {:status 400, :body {:error (.getMessage ^Exception error)}}))

(defmethod transform-query-resp "text/csv" [{:keys [results query] :as res} req]
  (or (handle-error res)
      (-> {:status 200, :body res}
          (with-download-header res "csv"))))

(defmethod transform-query-resp "text/tsv" [{:keys [results query] :as res} req]
  (or (handle-error res)
      ;; TODO what if query is a string?
      (-> {:status 200, :body res}
          (with-download-header res "tsv"))))

(defmethod transform-query-resp "text/html" [{:keys [error] :as res} _]
  {:status (if error 400 200)
   :body res})

(defmethod transform-query-resp :default [{:keys [results] :as res} _]
  (or (handle-error res)
      {:status 200, :body res}))

(defn data-browser-query [req {:keys [query-muuntaja] :as options}]
  (let [req (cond->> req
              (not (get-in req [:muuntaja/response :format])) (m/negotiate-and-format-request query-muuntaja))
        {:strs [valid-time transaction-time q] :as query-params} (:query-params req)]
    (-> (if (empty? query-params)
          (assoc options :no-query? true)
          (run-query (-> (or (some-> q (edn/read-string))
                             (build-query query-params))
                         (transform-query-req req))
                     (assoc options
                            :valid-time (when-not (string/blank? valid-time) (instant/read-instant-date valid-time))
                            :transaction-time (when-not (string/blank? transaction-time) (instant/read-instant-date transaction-time)))))
        (transform-query-resp req)
        (->> (m/format-response query-muuntaja req)))))
