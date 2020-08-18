(ns crux.http-server.entity
  (:require [crux.http-server.util :as util]
            [crux.http-server.entity-ref :as entity-ref]
            [cognitect.transit :as transit]
            [clojure.edn :as edn]
            [clojure.instant :as instant]
            [clojure.string :as string]
            [clojure.set :as set]
            [crux.api :as api]
            [crux.codec :as c]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [crux.api :as crux]
            [crux.io :as cio]
            [clojure.java.io :as io])
  (:import [crux.api ICruxAPI NodeOutOfSyncException]
           java.net.URLDecoder
           [java.time Duration Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.io.ByteArrayOutputStream
           java.util.Date
           crux.http_server.entity_ref.EntityRef))

(defn- entity-root-html []
  [:div.entity-root
   [:h1.entity-root__title
    "Browse Documents"]
   [:p "Fetch a specific entity by ID and browse its document history"]
   [:div.entity-root__contents
    [:div.entity-editor__title
     "Entity ID"]
    [:div.entity-editor__contents
     [:form
      {:action "/_crux/entity"}
      [:textarea.textarea
       {:name "eid"
        :placeholder "Enter an entity ID, found under the `:crux.db/id` key inside your documents"
        :rows 1}]
      [:div.entity-editor-datetime
       [:b "Valid Time"]
       [:input.input.input-time
        {:type "datetime-local"
         :name "valid-time"
         :step "0.01"
         :value (.format util/default-date-formatter (ZonedDateTime/now))}]
       [:b "Transaction Time"]
       [:input.input.input-time
        {:type "datetime-local"
         :name "transaction-time"
         :step "0.01"}]]
      [:button.button
       {:type "submit"}
       "Fetch Documents"]]]]])

(defn entity-links
  [db result]
  (letfn [(recur-on-result [result & key]
            (if (and (c/valid-id? result) (api/entity db result) (not= (first key) :crux.db/id))
              (entity-ref/->EntityRef result)
              (cond
                (map? result) (map (fn [[k v]] [k (recur-on-result v k)]) result)
                (sequential? result) (map recur-on-result result)
                :else result)))]
    (into {} (recur-on-result result))))

(defn resolve-entity-map [res entity-map]
  (if (instance? EntityRef entity-map)
    [:a {:href (entity-ref/EntityRef->url entity-map res)} (str (:eid entity-map))]
    (cond
      (map? entity-map) (for [[k v] entity-map]
                          ^{:key (str (gensym))}
                          [:div.entity-group
                           [:div.entity-group__key
                            (resolve-entity-map res k)]
                           [:div.entity-group__value
                            (resolve-entity-map res v)]])

      (sequential? entity-map) [:ol.entity-group__value
                                (for [v entity-map]
                                  ^{:key (str (gensym))}
                                  [:li (resolve-entity-map res v)])]
      (set? entity-map) [:ul.entity-group__value
                         (for [v entity-map]
                           ^{:key v}
                           [:li (resolve-entity-map res v)])]
      :else (str entity-map))))

(def ^DateTimeFormatter iso-format (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

(defn vt-tt-entity-box
  [vt tt]
  [:div.entity-vt-tt
   [:div.entity-vt-tt__title
    "Valid Time"]
   [:div.entity-vt-tt__value
    (->> (or ^Date vt (java.util.Date.))
         (.toInstant)
         ^ZonedDateTime ((fn [^Instant inst] (.atZone inst (ZoneId/of "Z"))))
         (.format iso-format)
         (str))]
   [:div.entity-vt-tt__title
    "Transaction Time"]
   [:div.entity-vt-tt__value
    (or (some-> ^Date tt
                (.toInstant)
                ^ZonedDateTime ((fn [^Instant inst] (.atZone inst (ZoneId/of "Z"))))
                (.format iso-format)
                (str))
        "Using Latest")]])

(defn- entity->html [{:keys [eid linked-entities entity valid-time transaction-time] :as res}]
  [:div.entity-map__container
   [:div.entity-map
    [:div.entity-group
     [:div.entity-group__key
      ":crux.db/id"]
     [:div.entity-group__value
      (str (:crux.db/id entity))]]
    [:hr.entity-group__separator]
    (resolve-entity-map linked-entities (dissoc entity :crux.db/id))]
   (vt-tt-entity-box valid-time transaction-time)])

(defn- entity-history->html [{:keys [eid entity-history]}]
  [:div.entity-histories__container
   [:div.entity-histories
    (for [{:keys [crux.tx/tx-time crux.db/valid-time crux.db/doc]} entity-history]
      [:div.entity-history__container
       [:div.entity-map
        (resolve-entity-map {} doc)]
       (vt-tt-entity-box valid-time tx-time)])]])

(defn ->entity-html-encoder [opts]
  (reify mfc/EncodeToBytes
    (encode-to-bytes [_ {:keys [eid no-entity? not-found? error entity entity-history] :as res} charset]
      (let [^String resp (cond
                           no-entity? (util/raw-html {:body (entity-root-html)
                                                      :title "/entity"
                                                      :options opts})
                           not-found? (let [not-found-message (str eid " entity not found")]
                                        (util/raw-html {:title "/entity"
                                                        :body [:div.error-box not-found-message]
                                                        :options opts
                                                        :results {:entity-results
                                                                  {"error" not-found-message}}}))
                           error (let [error-message (.getMessage ^Exception error)]
                                   (util/raw-html {:title "/entity"
                                                   :body [:div.error-box error-message]
                                                   :options opts
                                                   :results {:entity-results
                                                             {"error" error-message}}}))
                           entity-history (util/raw-html {:body (entity-history->html res)
                                                          :title "/entity?history=true"
                                                          :options opts
                                                          :results {:entity-results entity-history}})
                           :else (util/raw-html {:body (entity->html res)
                                                 :title "/entity"
                                                 :options opts
                                                 :results {:entity-results entity}}))]
        (.getBytes resp ^String charset)))))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity entity-history] _}]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (if entity-history
            (try
              (print-dup (iterator-seq entity-history))
              (finally
                (cio/try-close entity-history)))
            (print-dup entity w))))))))

(defn- ->tj-encoder [_]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ entity _]
      (let [baos (ByteArrayOutputStream.)
            writer (transit/writer baos :json {:handlers {EntityRef entity-ref/ref-write-handler}})]
        (transit/write writer entity)
        (.toByteArray baos)))))

(defn ->entity-muuntaja [opts]
  (m/create (-> m/default-options
                (dissoc :formats)
                (assoc :default-format "application/edn"
                       :return :output-stream)
                (m/install {:name "text/html"
                            :encoder [->entity-html-encoder opts]
                            :return :bytes})
                (m/install {:name "application/transit+json"
                            :encoder [->tj-encoder]})
                (m/install {:name "application/edn"
                            :encoder [->edn-encoder]}))))

(defn search-entity-history [{:keys [crux-node eid valid-time transaction-time sort-order history-opts limit]}]
  (try
    (let [eid (edn/read-string {:readers {'crux/id c/id-edn-reader}}
                               (URLDecoder/decode eid))
          db (util/db-for-request crux-node {:valid-time valid-time
                                             :transact-time transaction-time})
          entity-history (api/open-entity-history db eid sort-order history-opts)]
      (if-not (.hasNext entity-history)
        {:not-found? true}
        {:eid eid
         :valid-time (api/valid-time db)
         :transaction-time (api/transaction-time db)
         :entity-history (cio/fmap-cursor (fn [entity-history]
                                            (cond->> (map #(update % :crux.db/content-hash str) entity-history)
                                              limit (take limit)))
                                          entity-history)}))
    (catch Exception e
      {:error e})))

(defn search-entity [{:keys [crux-node eid valid-time transaction-time link-entities?] :as params}]
  (try
    (let [eid (edn/read-string {:readers {'crux/id c/id-edn-reader}}
                               (URLDecoder/decode eid))
          db (util/db-for-request crux-node {:valid-time valid-time
                                             :transact-time transaction-time})
          entity (api/entity db eid)]
      (merge {:eid eid
              :valid-time (api/valid-time db)
              :transaction-time (api/transaction-time db)}
             (cond
               (empty? entity) {:not-found? true}
               link-entities? {:entity (entity-links db entity)}
               :else {:entity entity})))
    (catch Exception e
      {:error e})))

(defn transform-query-params [{:keys [query-params] :as req}]
  (if (= "text/html" (get-in req [:muuntaja/response :format]))
    (assoc query-params "with-docs" "true" "link-entities?" "true")
    query-params))

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-resp "text/html" [{:keys [error no-entity? not-found? error] :as res} _]
  {:status (cond
             error 400
             not-found? 404
             :else 200)
   :body res})

(defmethod transform-query-resp :default [{:keys [eid error no-entity? not-found? entity entity-history] :as res} _]
  (cond
    no-entity? {:status 400, :body {:error "Missing eid"}}
    not-found? {:status 404, :body {:error (str eid " entity not found")}}
    error {:status 400, :body {:error (.getMessage ^Exception error)}}
    entity-history {:status 200, :body entity-history}
    :else {:status 200, :body entity}))

(defn entity-state [req {:keys [entity-muuntaja] :as options}]
  (let [req (m/negotiate-and-format-request entity-muuntaja req)
        {:strs [eid history sort-order limit
                valid-time transaction-time
                start-valid-time start-transaction-time
                end-valid-time end-transaction-time
                with-corrections with-docs link-entities?] :as query-params} (transform-query-params req)]
    (-> (if (nil? eid)
          (assoc options :no-entity? true)
          (let [entity-options (assoc options
                                      :eid eid
                                      :valid-time (when-not (string/blank? valid-time) (instant/read-instant-date valid-time))
                                      :transaction-time (when-not (string/blank? transaction-time) (instant/read-instant-date transaction-time)))]
            (if history
              (search-entity-history (assoc entity-options
                                            :sort-order (some-> sort-order keyword)
                                            :limit (some-> ^String limit Long/parseLong)
                                            :history-opts {:with-corrections? (some-> ^String with-corrections Boolean/valueOf)
                                                           :with-docs? (some-> ^String with-docs Boolean/valueOf)
                                                           :start {:crux.db/valid-time (some-> start-valid-time (instant/read-instant-date))
                                                                   :crux.tx/tx-time (some-> start-transaction-time (instant/read-instant-date))}
                                                           :end {:crux.db/valid-time (some-> end-valid-time (instant/read-instant-date))
                                                                 :crux.tx/tx-time (some-> end-transaction-time (instant/read-instant-date))}}))
              (search-entity (assoc entity-options
                                    :link-entities? (some-> ^String link-entities? Boolean/valueOf))))))
        (transform-query-resp req)
        (->> (m/format-response entity-muuntaja req)))))
