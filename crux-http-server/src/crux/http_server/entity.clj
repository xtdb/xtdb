(ns crux.http-server.entity
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [cognitect.transit :as transit]
            [crux.api :as crux]
            [crux.codec :as c]
            [crux.error :as ce]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.http-server.util :as util]
            [crux.io :as cio]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc])
  (:import crux.codec.Id
           crux.http_server.entity_ref.EntityRef
           crux.io.Cursor
           [java.io Closeable OutputStream]
           [java.time Instant ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date))

(s/def ::sort-order keyword?)
(s/def ::start-valid-time inst?)
(s/def ::end-valid-time inst?)
(s/def ::start-transaction-time inst?)
(s/def ::end-transaction-time inst?)
(s/def ::history boolean?)
(s/def ::with-corrections boolean?)
(s/def ::with-docs boolean?)
(s/def ::query-params
  (s/keys :opt-un [::util/eid
                   ::history
                   ::sort-order
                   ::util/valid-time
                   ::util/transaction-time
                   ::start-valid-time
                   ::start-transaction-time
                   ::end-valid-time
                   ::end-transaction-time
                   ::with-corrections
                   ::with-docs
                   ::util/link-entities?]))

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
            (if (and (c/valid-id? result) (crux/entity db result) (not= (first key) :crux.db/id))
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
    (encode-to-bytes [_ {:keys [eid no-entity? not-found? entity ^Closeable entity-history] :as res} charset]
      (let [^String resp (cond
                           no-entity? (util/raw-html {:body (entity-root-html)
                                                      :title "/_crux/entity"
                                                      :options opts})
                           not-found? (let [not-found-message (str eid " entity not found")]
                                        (util/raw-html {:title "/_crux/entity"
                                                        :body [:div.error-box not-found-message]
                                                        :options opts
                                                        :results {:entity-results
                                                                  {"error" not-found-message}}}))
                           entity-history (try
                                            (util/raw-html {:body (entity-history->html res)
                                                            :title "/_crux/entity?history=true"
                                                            :options opts
                                                            :results {:entity-results (iterator-seq entity-history)}})
                                            (finally
                                              (.close entity-history)))
                           entity (util/raw-html {:body (entity->html res)
                                                  :title "/_crux/entity"
                                                  :options opts
                                                  :results {:entity-results entity}})
                           :else (util/raw-html {:title "/_crux/entity"
                                                 :body [:div.error-box (str res)]
                                                 :options opts
                                                 :results {:entity-results
                                                           {"error" res}}}))]
        (.getBytes resp ^String charset)))))

(defn ->edn-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity ^Cursor entity-history] :as res} _]
      (fn [^OutputStream output-stream]
        (with-open [w (io/writer output-stream)]
          (cond
            entity-history (try
                             (print-method (or (iterator-seq entity-history) '()) w)
                             (finally
                               (cio/try-close entity-history)))
            entity (print-method entity w)
            :else (.write w ^String (pr-str res))))))))

(defn- ->tj-encoder [_]
  (reify
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [entity ^Cursor entity-history] :as res} _]
      (fn [^OutputStream output-stream]
        (let [w (transit/writer output-stream :json {:handlers {EntityRef entity-ref/ref-write-handler
                                                                Id util/crux-id-write-handler}})]
          (cond
            entity (transit/write w entity)
            entity-history (try
                             (transit/write w (or (iterator-seq entity-history) '()))
                             (finally
                               (cio/try-close entity-history)))
            :else (transit/write w res)))))))

(defn ->entity-muuntaja [opts]
  (m/create (-> m/default-options
                (dissoc :formats)
                (assoc :return :output-stream
                       :default-format "application/edn")
                (m/install {:name "text/html"
                            :encoder [->entity-html-encoder opts]
                            :return :bytes})
                (m/install {:name "application/transit+json"
                            :encoder [->tj-encoder]})
                (m/install {:name "application/edn"
                            :encoder [->edn-encoder]}))))

(defn search-entity-history [{:keys [crux-node]} {:keys [eid valid-time transaction-time sort-order with-corrections with-docs
                                                         start-valid-time start-transaction-time end-valid-time end-transaction-time]}]
  (try
    (let [db (util/db-for-request crux-node {:valid-time valid-time
                                             :transact-time transaction-time})
          history-opts {:with-corrections? with-corrections
                        :with-docs? with-docs
                        :start {:crux.db/valid-time start-valid-time
                                :crux.tx/tx-time start-transaction-time}
                        :end {:crux.db/valid-time end-valid-time
                              :crux.tx/tx-time end-transaction-time}}
          entity-history (crux/open-entity-history db eid sort-order history-opts)]
      {:entity-history entity-history})
    (catch Exception e
      {:error e})))

(defn search-entity [{:keys [crux-node]} {:keys [eid valid-time transaction-time link-entities?]}]
  (try
    (let [db (util/db-for-request crux-node {:valid-time valid-time
                                             :transact-time transaction-time})
          entity (crux/entity db eid)]
      (cond
        (empty? entity) {:eid eid :not-found? true}
        link-entities? {:entity (entity-links db entity)}
        :else {:entity entity}))
    (catch Exception e
      {:error e})))

(defn transform-query-params [req]
  (let [query-params (get-in req [:parameters :query])]
    (if (= "text/html" (get-in req [:muuntaja/response :format]))
      (assoc query-params :with-docs true :link-entities? true)
      query-params)))

(defmulti transform-query-resp
  (fn [resp req]
    (get-in req [:muuntaja/response :format])))

(defmethod transform-query-resp "text/html" [{:keys [error no-entity? not-found? error] :as res} _]
  (cond
    error (throw error)
    :else {:status (cond
                     no-entity? 400
                     not-found? 404
                     :else 200)
           :body res}))

(defmethod transform-query-resp :default [{:keys [error no-entity? eid not-found?] :as res} _]
  (cond
    no-entity? (throw (ce/illegal-arg :missing-eid {::ce/message "Missing eid"}))
    not-found? {:status 404, :body (str eid " entity not found")}
    error (throw error)
    :else {:status 200, :body res}))

(defn entity-state [options]
  (fn [req]
    (let [{:keys [eid history] :as query-params} (transform-query-params req)]
      (-> (cond
            (nil? eid) {:no-entity? true}
            history (search-entity-history options query-params)
            :else (search-entity options query-params))
          (transform-query-resp req)))))
