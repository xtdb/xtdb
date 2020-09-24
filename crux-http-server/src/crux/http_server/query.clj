(ns crux.http-server.query
  (:require [crux.http-server.util :as util]
            [crux.http-server.entity-ref :as entity-ref]
            [cognitect.transit :as transit]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.instant :as instant]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.api :as crux]
            [crux.codec :as c]
            [crux.error :as ce]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [muuntaja.format.edn :as mfe]
            [muuntaja.format.transit :as mft]
            [crux.io :as cio]
            [crux.db :as db]
            [crux.query :as q]
            [clojure.spec.alpha :as s]
            [spec-tools.core :as st]
            [crux.error :as err])
  (:import crux.http_server.entity_ref.EntityRef
           crux.io.Cursor
           (java.io OutputStream Writer)
           [java.time Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date))

(s/def ::query
  (st/spec
   {:spec #(s/valid? ::q/query %)
    :type :map
    :decode/string (fn [_ q] (util/try-decode-edn q))}))

;; TODO: Need to ensure all query clasues are present + coerced properly
(s/def ::query-params
  (s/keys :opt-un [::util/valid-time ::util/transaction-time ::util/link-entities? ::query]))

(s/def ::args (s/coll-of any? :kind vector?))

(s/def ::body-params
  (s/keys :req-un [::q/query]
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

(defn- query-root-html [{:keys [crux-node]}]
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
       {:name "query"
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
          :step "0.01"
          :value (some-> (crux/latest-completed-tx crux-node)
                         :crux.tx/tx-time
                         ((fn [tx-time] (.toInstant ^Date tx-time)))
                         (ZonedDateTime/ofInstant (ZoneId/of "Z"))
                         (->> (.format util/default-date-formatter )))}]]]
      [:button.button
       {:type "submit"}
       "Submit Query"]]]]])

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

(defn ->html-encoder [opts]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [no-query? results] :as res} charset]
      (try
        (let [^String resp (cond
                             no-query? (util/raw-html {:body (query-root-html opts)
                                                       :title "/_crux/query"
                                                       :options opts})
                             results (let [results (iterator-seq results)]
                                       (util/raw-html {:body (query->html (assoc res :results results))
                                                       :title "/_crux/query"
                                                       :options opts
                                                       :results {:query-results results}}))
                             :else (util/raw-html {:title "/_crux/query"
                                                   :body [:div.error-box (str res)]
                                                   :options opts
                                                   :results {:query-results
                                                             {"error" res}}}) )]
          (.getBytes resp ^String charset))
        (finally
          (cio/try-close results))))))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as res} _]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (try
            (cond
              (and results (.hasNext results)) (print-method (iterator-seq results) w)
              results (.write w ^String (pr-str '()))
              :else (.write w ^String (pr-str res)))
            (finally
              (cio/try-close results))))))))

(defn- ->tj-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as res} _]
      (fn [^OutputStream output-stream]
        (let [w (transit/writer output-stream :json {:handlers {EntityRef entity-ref/ref-write-handler}})]
          (try
            (cond
              (and results (.hasNext results)) (transit/write w (iterator-seq results))
              results (transit/write w '())
              :else (transit/write w res))
            (finally
              (cio/try-close results))))))))

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
                            :encoder [->edn-encoder]
                            :decoder [mfe/decoder]})
                (m/install {:name "application/transit+json"
                            :encoder [->tj-encoder]
                            :decoder [(partial mft/decoder :json)]}))))

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
    no-query? (throw (ce/illegal-arg :no-query {::ce/message "No query provided"}))
    :else (-> {:status 200, :body res}
              (with-download-header res "csv"))))

(defmethod transform-query-resp "text/tsv" [{:keys [no-query?] :as res} _]
  (cond
    no-query? (throw (ce/illegal-arg :no-query {::ce/message "No query provided"}))
    :else (-> {:status 200, :body res}
              (with-download-header res "tsv"))))

(defmethod transform-query-resp "text/html" [res _]
  {:status 200 :body res})

(defmethod transform-query-resp :default [{:keys [no-query?] :as res} _]
  (cond
    no-query? (throw (ce/illegal-arg :no-query {::ce/message "No query provided"}))
    :else {:status 200, :body res}))

(defn data-browser-query [options]
  (fn [req]
    (let [{query-params :query body-params :body} (get-in req [:parameters])
          {:keys [valid-time transaction-time query]} query-params
          query (or query (get body-params :query))]
      (-> (if (nil? query)
            (assoc options :no-query? true)
            (run-query (transform-req query req)
                       (assoc options
                              :valid-time valid-time
                              :transaction-time transaction-time)))
          (transform-query-resp req)))))
