(ns crux.http-server
  "HTTP API for Crux.

  The optional SPARQL handler requires juxt.crux/rdf."
  (:require [clojure.edn :as edn]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.instant :as instant]
            [clojure.walk :as walk]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.tx :as tx]
            [ring.adapter.jetty :as j]
            [ring.middleware.resource :refer [wrap-resource]]
            [muuntaja.middleware :refer [wrap-format]]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [ring.middleware.params :as p]
            [ring.util.io :as rio]
            [ring.util.request :as req]
            [ring.util.response :as resp]
            [ring.util.time :as rt]
            [hiccup2.core :as hiccup2])
  (:import [crux.api ICruxAPI ICruxDatasource NodeOutOfSyncException]
           [java.io Closeable IOException OutputStream]
           [java.time Duration ZonedDateTime ZoneId Instant]
           java.util.Date
           java.time.format.DateTimeFormatter
           [java.net URLDecoder URLEncoder]
           org.eclipse.jetty.server.Server
           [com.nimbusds.jose.jwk JWK JWKSet KeyType RSAKey ECKey]
           com.nimbusds.jose.JWSObject
           com.nimbusds.jwt.SignedJWT
           [com.nimbusds.jose.crypto RSASSAVerifier ECDSAVerifier]))

;; ---------------------------------------------------
;; Utils

(defn- raw-html
  [{:keys [body title options results]}]
  (str (hiccup2/html
        [:html
         {:lang "en"}
         [:head
          [:meta {:charset "utf-8"}]
          [:meta {:http-equiv "X-UA-Compatible" :content "IE=edge,chrome=1"}]
          [:meta
           {:name "viewport"
            :content "width=device-width, initial-scale=1.0, maximum-scale=1.0"}]
          [:link {:rel "icon" :href "/favicon.ico" :type "image/x-icon"}]
          (when options [:meta {:title "options" :content (str options)}])
          (when results [:meta {:title "results" :content (str results)}])
          [:link {:rel "stylesheet" :href "/css/all.css"}]
          [:link {:rel "stylesheet" :href "/latofonts.css"}]
          [:link {:rel "stylesheet" :href "/css/table.css"}]
          [:link {:rel "stylesheet" :href "/css/codemirror.css"}]
          [:link {:rel "stylesheet"
                  :href "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css"}]
          [:title "Crux Console"]]
         [:body
          [:nav.header
           [:div.crux-logo
            [:a {:href "/_crux/index"}
             [:img.crux-logo__img {:src "/crux-horizontal-bw.svg.png" }]]]
           [:span [:b (or (str "“" (:crux.http-server/label options) "”") "")]]
           [:div.header__links
            [:a.header__link {:href "/_crux/query"} "Query"]
            [:a.header__link {:href "/_crux/status"} "Status"]
            [:div.header-dropdown
             [:button.header-dropdown__button
              "Help"
              [:i.fa.fa-caret-down]]
             [:div.header-dropdown__links
              [:a {:href "https://opencrux.com/docs" :target "_blank"} "Documentation"]
              [:a {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux" :target "_blank"} "Zulip Chat"]
             ;; [:a {:href "https://clojurians.slack.com/messages/crux" :target "_blank"} "Clojurians Slack"]
              [:a {:href "mailto:crux@juxt.pro" :target "_blank"} "Email Support"]]]]]
          [:div.console
           [:div#app
            [:div.container.page-pane body]]]
          [:script {:src "/cljs-out/dev-main.js" :type "text/javascript"}]]])))

(defn- body->edn [request]
  (->> request
       req/body-string
       (c/read-edn-string-with-readers)))

(defn- check-path [[path-pattern valid-methods] request]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (re-find path-pattern path)
         (some #{method} valid-methods))))

(defn- response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn- redirect-response [url]
  {:status 302
   :headers {"Location" url}})

(defn- success-response [m]
  (response (if (some? m) 200 404)
            {"Content-Type" "application/edn"}
            (cio/pr-edn-str m)))

(defn- exception-response [status ^Exception e]
  (response status
            {"Content-Type" "application/edn"}
            (with-out-str
              (pp/pprint (Throwable->map e)))))

(defn- wrap-exception-handling [handler]
  (fn [request]
    (try
      (try
        (handler request)
        (catch Exception e
          (if (or (instance? IllegalArgumentException e)
                  (and (.getMessage e)
                       (str/starts-with? (.getMessage e) "Spec assertion failed")))
            (exception-response 400 e) ;; Valid edn, invalid content
            (do (log/error e "Exception while handling request:" (cio/pr-edn-str request))
                (exception-response 500 e))))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

(defn- add-last-modified [response date]
  (cond-> response
    date (assoc-in [:headers "Last-Modified"] (rt/format-date date))))

;; ---------------------------------------------------
;; Services


(defn- db-for-request ^ICruxDatasource [^ICruxAPI crux-node {:keys [valid-time transact-time]}]
  (cond
    (and valid-time transact-time)
    (.db crux-node valid-time transact-time)

    valid-time
    (.db crux-node valid-time)

    ;; TODO: This could also be an error, depending how you see it,
    ;; not supported via the Java API itself.
    transact-time
    (.db crux-node (cio/next-monotonic-date) transact-time)

    :else
    (.db crux-node)))

(defn- streamed-edn-response [^Closeable ctx edn]
  (try
    (->> (rio/piped-input-stream
          (fn [out]
            (with-open [ctx ctx
                        out (io/writer out)]
              (.write out "(")
              (doseq [x edn]
                (.write out (cio/pr-edn-str x)))
              (.write out ")"))))
         (response 200 {"Content-Type" "application/edn"}))
    (catch Throwable t
      (.close ctx)
      (throw t))))

(def ^:private date? (partial instance? Date))
(s/def ::valid-time date?)
(s/def ::transact-time date?)

(s/def ::query-map (s/and #(set/superset? #{:query :valid-time :transact-time} (keys %))
                          (s/keys :req-un [:crux.query/query]
                                  :opt-un [::valid-time
                                           ::transact-time])))

(defn- validate-or-throw [body-edn spec]
  (when-not (s/valid? spec body-edn)
    (throw (ex-info (str "Spec assertion failed\n" (s/explain-str spec body-edn)) (s/explain-data spec body-edn)))))

(defn- query [^ICruxAPI crux-node request]
  (let [query-map (doto (body->edn request) (validate-or-throw ::query-map))
        db (db-for-request crux-node query-map)
        result (api/open-q db (:query query-map))]
    (-> (streamed-edn-response result (iterator-seq result))
        (add-last-modified (.transactionTime db)))))

(s/def ::eid c/valid-id?)
(s/def ::entity-map (s/and (s/keys :opt-un [::valid-time ::transact-time])))

(defn- entity [^ICruxAPI crux-node {:keys [query-params] :as request}]
  (let [body (doto (body->edn request) (validate-or-throw ::entity-map))
        eid (or (:eid body)
                (some-> (re-find #"^/entity/(.+)$" (req/path-info request))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))
        db (db-for-request crux-node {:valid-time (or (:valid-time body)
                                                      (some-> (get query-params "valid-time")
                                                              (instant/read-instant-date)))
                                      :transact-time (or (:transact-time body)
                                                         (some-> (get query-params "transaction-time")
                                                                 (instant/read-instant-date)))})
        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response (.entity db eid))
        (add-last-modified tx-time))))

(defn- entity-tx [^ICruxAPI crux-node {:keys [query-params] :as request}]
  (let [body (doto (body->edn request) (validate-or-throw ::entity-map))
        eid (or (:eid body)
                (some-> (re-find #"^/entity-tx/(.+)$" (req/path-info request))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))

        db (db-for-request crux-node {:valid-time (or (:valid-time body)
                                                      (some-> (get query-params "valid-time")
                                                              (instant/read-instant-date)))
                                      :transact-time (or (:transact-time body)
                                                         (some-> (get query-params "transaction-time")
                                                                 (instant/read-instant-date)))})

        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response entity-tx)
        (add-last-modified tx-time))))

(defn- entity-history [^ICruxAPI node
                       {{:strs [sort-order
                                valid-time transaction-time
                                start-valid-time start-transaction-time
                                end-valid-time end-transaction-time
                                with-corrections with-docs]} :query-params
                        :as req}]
  (let [db (db-for-request node {:valid-time (some-> valid-time (instant/read-instant-date))
                                 :transact-time (some-> transaction-time (instant/read-instant-date))})
        eid (or (some-> (re-find #"^/entity-history/(.+)$" (req/path-info req))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))
        sort-order (or (some-> sort-order keyword)
                       (throw (IllegalArgumentException. "missing sort-order query parameter")))
        opts {:with-corrections? (some-> ^String with-corrections Boolean/valueOf)
              :with-docs? (some-> ^String with-docs Boolean/valueOf)
              :start {:crux.db/valid-time (some-> start-valid-time (instant/read-instant-date))
                      :crux.tx/tx-time (some-> start-transaction-time (instant/read-instant-date))}
              :end {:crux.db/valid-time (some-> end-valid-time (instant/read-instant-date))
                    :crux.tx/tx-time (some-> end-transaction-time (instant/read-instant-date))}}
        history (api/open-entity-history db eid sort-order opts)]
    (-> (streamed-edn-response history (iterator-seq history))
        (add-last-modified (:crux.tx/tx-time (api/latest-completed-tx node))))))

(defn- transact [^ICruxAPI crux-node request]
  (let [tx-ops (body->edn request)
        {:keys [crux.tx/tx-time] :as submitted-tx} (.submitTx crux-node tx-ops)]
    (-> (success-response submitted-tx)
        (assoc :status 202)
        (add-last-modified tx-time))))

;; TODO: Could add from date parameter.
(defn- tx-log [^ICruxAPI crux-node request]
  (let [with-ops? (Boolean/parseBoolean (get-in request [:query-params "with-ops"]))
        after-tx-id (some->> (get-in request [:query-params "after-tx-id"])
                             (Long/parseLong))
        result (.openTxLog crux-node after-tx-id with-ops?)]
    (-> (streamed-edn-response result (iterator-seq result))
        (add-last-modified (:crux.tx/tx-time (.latestCompletedTx crux-node))))))

(defn- sync-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        ;; TODO this'll get cut down with the rest of the sync deprecation
        transaction-time (some->> (get-in request [:query-params "transactionTime"])
                                  (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (if transaction-time
                          (.awaitTxTime crux-node transaction-time timeout)
                          (.sync crux-node timeout))]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-time-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-time (some->> (get-in request [:query-params "tx-time"])
                         (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (.awaitTxTime crux-node tx-time timeout)]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-id (-> (get-in request [:query-params "tx-id"])
                  (Long/parseLong))]
    (let [{:keys [crux.tx/tx-time] :as tx} (.awaitTx crux-node {:crux.tx/tx-id tx-id} timeout)]
      (-> (success-response tx)
          (add-last-modified tx-time)))))

(defn- attribute-stats [^ICruxAPI crux-node]
  (success-response (.attributeStats crux-node)))

(defn- tx-committed? [^ICruxAPI crux-node request]
  (try
    (let [tx-id (-> (get-in request [:query-params "tx-id"])
                    (Long/parseLong))]
      (success-response (.hasTxCommitted crux-node {:crux.tx/tx-id tx-id})))
    (catch NodeOutOfSyncException e
      (exception-response 400 e))))

(defn latest-completed-tx [^ICruxAPI crux-node]
  (success-response (.latestCompletedTx crux-node)))

(defn latest-submitted-tx [^ICruxAPI crux-node]
  (success-response (.latestSubmittedTx crux-node)))

(def ^:private sparql-available?
  (try ; you can change it back to require when clojure.core fixes it to be thread-safe
    (requiring-resolve 'crux.sparql.protocol/sparql-query)
    true
    (catch IOException _
      false)))

;; ---------------------------------------------------
;; Jetty server

(defn- handler [request {:keys [crux-node ::read-only?]}]
  (condp check-path request
    [#"^/$" [:get]]
    (redirect-response "/_crux/index")

    [#"^/entity/.+$" [:get]]
    (entity crux-node request)

    [#"^/entity-tx/.+$" [:get]]
    (entity-tx crux-node request)

    [#"^/entity$" [:post]]
    (entity crux-node request)

    [#"^/entity-tx$" [:post]]
    (entity-tx crux-node request)

    [#"^/entity-history/.+$" [:get]]
    (entity-history crux-node request)

    [#"^/query$" [:post]]
    (query crux-node request)

    [#"^/attribute-stats" [:get]]
    (attribute-stats crux-node)

    [#"^/sync$" [:get]]
    (sync-handler crux-node request)

    [#"^/await-tx$" [:get]]
    (await-tx-handler crux-node request)

    [#"^/await-tx-time$" [:get]]
    (await-tx-time-handler crux-node request)

    [#"^/tx-log$" [:get]]
    (tx-log crux-node request)

    [#"^/tx-log$" [:post]]
    (if read-only?
      (-> (resp/response "forbidden: read-only HTTP node")
          (resp/status 403))
      (transact crux-node request))

    [#"^/tx-committed$" [:get]]
    (tx-committed? crux-node request)

    [#"^/latest-completed-tx" [:get]]
    (latest-completed-tx crux-node)

    [#"^/latest-submitted-tx" [:get]]
    (latest-submitted-tx crux-node)

    (if (and (check-path [#"^/sparql/?$" [:get :post]] request)
             sparql-available?)
      ((resolve 'crux.sparql.protocol/sparql-query) crux-node request)
      nil)))

(defn html-request? [request]
  (= (get-in request [:muuntaja/response :format]) "text/html"))

(defn- root-page []
  [:div.root-page
   [:div.root-background]
   [:div.root-contents
    [:h1.root-title "Console Overview"]
    ;;[:h2.root-subtitle "Crux Console"]
    [:div.root-info-summary
     [:div.root-info "✓ Crux node is active"]
     [:div.root-info "✓ HTTP is enabled"]]
    ;;[:div.root-info "✓ version"]
   ;; [:h2.root-subtitle "There are N indexed documents"]
   ;; [:h2.root-subtitle "There are have been T transactions"]
   ;; [:h2.root-subtitle "This node is read-only over HTTP"]
    [:div.root-tiles
     [:a.root-tile
      {:href "/_crux/query"}
      [:i.fas.fa-search]
      [:br]
      "Query"]
     [:a.root-tile
      {:href "/_crux/status"}
      [:i.fas.fa-wrench]
      [:br]
      "Status"]
     [:a.root-tile
      {:href "https://opencrux.com/docs" :target "_blank"}
      [:i.fas.fa-book]
      [:br]
      "Docs"]]
    ]])

(defn- root-handler [^ICruxAPI crux-node options request]
  {:status 200
   :headers {"Content-Location" "/_crux/index.html"}
   :body (when (html-request? request)
           (raw-html
            {:body (root-page)
             :title "/_crux"
             :options options}))})

(defn sort-map [map]
  (->> map
       (walk/postwalk
        (fn [map] (cond->> map
                  (map? map) (into (sorted-map)))))))

(defn attribute-stats->html-elements [stats-map]
  [:dl.node-info__content
   [:table.table
    [:thead.table__head
     [:th "Attribute"]
     [:th "Latest Count (across all document versions)"]]
    (into
     [:tbody.table__body]
     (map
      (fn [[key value]]
        (when value
          [:tr.table__row.body__row
           [:td.table__cell.body__cell (str key)]
           [:td.table__cell.body__cell (with-out-str (pp/pprint value))]]))
      (sort-by (juxt val key) #(compare %2 %1) stats-map)))]])

(defn status-map->html-elements [status-map]
  (into
   [:dl.node-info__content]
   (mapcat
    (fn [[key value]]
      (when value
        [[:dt [:b (str key)]]
         (cond
           (map? value) [:dd (into
                              [:dl]
                              (mapcat
                               (fn [[key value]]
                                 [[:dt [:b (str key)]]
                                  [:dd (with-out-str (pp/pprint value))]])
                               value))]
           :else [:dd (with-out-str (pp/pprint value))])]))
    (sort-map status-map))))

(defn- status [^ICruxAPI crux-node options request]
  (let [status-map (api/status crux-node)]
    {:status (if (or (not (contains? status-map :crux.zk/zk-active?))
                     (:crux.zk/zk-active? status-map))
               200
               500)
     :body (if (html-request? request)
             (let [attribute-stats (api/attribute-stats crux-node)]
               (raw-html
                {:body [:div.node-info__container
                        [:h1 "Status"]
                        [:div.node-info
                         [:h2.node-info__title "Overview"]
                         (status-map->html-elements status-map)]
                        [:div.node-info
                         [:h2.node-info__title "Current Configuration"]
                         (status-map->html-elements options)]
                        [:div.node-info
                         [:h2.node-info__title "Attribute Cardinalities"]
                         (attribute-stats->html-elements attribute-stats)]]
                 :title "/_status"
                 :options options}))
             status-map)}))

(def ^DateTimeFormatter default-date-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS"))

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
         :value (.format default-date-formatter (ZonedDateTime/now))}]
       [:b "Transaction Time"]
       [:input.input.input-time
        {:type "datetime-local"
         :name "transaction-time"
         :step "0.01"}]]
      [:button.button
       {:type "submit"}
       "Fetch Documents"]]]]])

(defn link-all-entities
  [db path result]
  (letfn [(recur-on-result [result links]
            (if (and (c/valid-id? result)
                     (api/entity db result))
              (let [encoded-eid (URLEncoder/encode (pr-str result) "UTF-8")
                    query-params (format "?eid=%s&valid-time=%s&transaction-time=%s"
                                         encoded-eid
                                         (.toInstant ^Date (api/valid-time db))
                                         (.toInstant ^Date (api/transaction-time db)))]
                (assoc links result (str path query-params)))
              (cond
                (map? result) (apply merge (map #(recur-on-result % links) (vals result)))
                (sequential? result) (apply merge (map #(recur-on-result % links) result))
                :else links)))]
    (recur-on-result result {})))

(defn resolve-entity-map [linked-entities entity-map]
  (if-let [href (get linked-entities entity-map)]
    [:a
     {:href href}
     (str entity-map)]
    (cond
      (map? entity-map) (for [[k v] entity-map]
                          ^{:key (str (gensym))}
                          [:div.entity-group
                           [:div.entity-group__key
                            (resolve-entity-map linked-entities k)]
                           [:div.entity-group__value
                            (resolve-entity-map linked-entities v)]])

      (sequential? entity-map) [:ol.entity-group__value
                                (for [v entity-map]
                                  ^{:key (str (gensym))}
                                  [:li (resolve-entity-map linked-entities v)])]
      (set? entity-map) [:ul.entity-group__value
                         (for [v entity-map]
                           ^{:key v}
                           [:li (resolve-entity-map linked-entities v)])]
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

(defn- entity->html [eid linked-entities entity-map vt tt]
  [:div.entity-map__container
   (if entity-map
     [:div.entity-map
      [:div.entity-group
       [:div.entity-group__key
        ":crux.db/id"]
       [:div.entity-group__value
        (str (:crux.db/id entity-map))]]
      [:hr.entity-group__separator]
      (resolve-entity-map (linked-entities) (dissoc entity-map :crux.db/id))]
     [:div.entity-map
      [:strong (str eid)] " entity not found"])
   (vt-tt-entity-box vt tt)])

(defn- entity-history->html [eid entity-history]
  [:div.entity-histories__container
   [:div.entity-histories
    (if (not-empty entity-history)
      (for [{:keys [crux.tx/tx-time crux.db/valid-time crux.db/doc]} entity-history]
        [:div.entity-history__container
         [:div.entity-map
          (resolve-entity-map {} doc)]
         (vt-tt-entity-box valid-time tx-time)])
      [:div.entity-histories
       [:strong (str eid)] " entity not found"])]])

(defn- entity-state [^ICruxAPI crux-node options {{:strs [eid history sort-order
                                                          valid-time transaction-time
                                                          start-valid-time start-transaction-time
                                                          end-valid-time end-transaction-time
                                                          with-corrections with-docs link-entities?]} :query-params
                                                  :as request}]
  (let [html? (html-request? request)]
    (if (nil? eid)
      (if html?
        {:status 200
         :body (raw-html
                {:body (entity-root-html)
                 :title "/entity"
                 :options options})}
        (throw (IllegalArgumentException. "missing eid")))
      (let [decoded-eid (edn/read-string {:readers {'crux/id c/id-edn-reader}}
                                         (URLDecoder/decode eid))
            vt (when-not (str/blank? valid-time) (instant/read-instant-date valid-time))
            tt (when-not (str/blank? transaction-time) (instant/read-instant-date transaction-time))
            db (db-for-request crux-node {:valid-time vt
                                          :transact-time tt})]
        (if history
          (let [sort-order (or (some-> sort-order keyword)
                               (throw (IllegalArgumentException. "missing sort-order query parameter")))
                history-opts {:with-corrections? (some-> ^String with-corrections Boolean/valueOf)
                              :with-docs? (or html? (some-> ^String with-docs Boolean/valueOf))
                              :start {:crux.db/valid-time (some-> start-valid-time (instant/read-instant-date))
                                      :crux.tx/tx-time (some-> start-transaction-time (instant/read-instant-date))}
                              :end {:crux.db/valid-time (some-> end-valid-time (instant/read-instant-date))
                                    :crux.tx/tx-time (some-> end-transaction-time (instant/read-instant-date))}}
                entity-history (api/entity-history db decoded-eid sort-order history-opts)]
            {:status (if (not-empty entity-history) 200 404)
             :body (let [edn-results (map #(update % :crux.db/content-hash str) entity-history)]
                     (if html?
                       (raw-html
                        {:body (entity-history->html eid entity-history)
                         :title "/entity?history=true"
                         :options options
                         :results {:entity-results edn-results}})
                       ;; Stringifying #crux/id values, caused issues with AJAX
                       edn-results))})
          (let [entity-map (api/entity db decoded-eid)
                linked-entities #(link-all-entities db  "/_crux/entity" entity-map)]
            {:status (if (some? entity-map) 200 404)
             :body (cond
                     html? (raw-html
                            {:body (entity->html eid linked-entities entity-map vt tt)
                             :title "/entity"
                             :options options
                             :results {:entity-results {"linked-entities" (linked-entities)
                                                        "entity" entity-map}}})

                     link-entities? {"linked-entities" (linked-entities)
                                     "entity" entity-map}

                     :else entity-map)}))))))

(def query-root-str
  (clojure.string/join "\n"
               [";; To perform a query:"
                ";; 1) Enter a query into this query editor, such as the following example"
                ";; 2) Optionally, select a \"valid time\" and/or \"transaction time\" to query against"
                ";; 3) Submit the query and the tuple results will be displayed in a table below"
                ""
                "{"
                " :find [?e]                ;; return a set of tuples each consisting of a unique ?e value"
                " :where [[?e :crux.db/id]] ;; select ?e as the entity id for all entities in the database"
                " :limit 100                ;; limit the initial page of results to keep things snappy"
                "}"]))

(defn- query-root-html []
  [:div.query-root
   [:h1.query-root__title
    "Query"]
   [:div.query-root__contents
    [:p "Enter a "
     [:a {:href "https://www.opencrux.com/docs#queries_basic_query" :target "_blank"} "Datalog"]
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
          :value (.format default-date-formatter (ZonedDateTime/now (ZoneId/of "Z")))}]]
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

(defn- build-query [{:strs [find where args order-by limit offset full-results]}]
  (let [new-offset (if offset
                     (Integer/parseInt offset)
                     0)]
    (cond-> {:find (c/read-edn-string-with-readers find)
             :where (->> where vectorize-param (mapv c/read-edn-string-with-readers))
             :offset new-offset}
      args (assoc :args (->> args vectorize-param (mapv c/read-edn-string-with-readers)))
      order-by (assoc :order-by (->> order-by vectorize-param (mapv c/read-edn-string-with-readers)))
      limit (assoc :limit (Integer/parseInt limit))
      full-results (assoc :full-results? true))))

(defn link-top-level-entities
  [db path results]
  (->> (apply concat results)
       (filter (every-pred c/valid-id? #(api/entity db %)))
       (map (fn [id]
              (let [encoded-eid (URLEncoder/encode (pr-str id) "UTF-8")
                    query-params (format "?eid=%s&valid-time=%s&transaction-time=%s"
                                         encoded-eid
                                         (.toInstant ^Date (api/valid-time db))
                                         (.toInstant ^Date (api/transaction-time db)))]
                [id (str path query-params)])))
       (into {})))

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

(defn query->html [links {headers :find} results]
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
               (if-let [href (get links cell-value)]
                 [:a {:href href} (str cell-value)]
                 (str cell-value))])])]
        [:tbody.table__body.table__no-data
         [:tr [:td.td__no-data
               "Nothing to show"]]])]]
    [:table.table__foot]]])

(defn data-browser-query [^ICruxAPI crux-node options {{:strs [valid-time transaction-time q]} :query-params :as request}]
  (let [query-params (:query-params request)
        link-entities? (get query-params "link-entities?")
        html? (html-request? request)
        csv? (= (get-in request [:muuntaja/response :format]) "text/csv")
        tsv? (= (get-in request [:muuntaja/response :format]) "text/tsv")]
    (cond
      (empty? query-params)
      (if html?
        {:status 200
         :body (raw-html
                {:body (query-root-html)
                 :title "/query"
                 :options options})}
        {:status 400
         :body "No query provided."})

      :else
      (try
        (let [query (cond-> (or (some-> q (edn/read-string))
                                (build-query query-params))
                      (or html? csv? tsv?) (dissoc :full-results?))
              vt (when-not (str/blank? valid-time) (instant/read-instant-date valid-time))
              tt (when-not (str/blank? transaction-time) (instant/read-instant-date transaction-time))
              db (db-for-request crux-node {:valid-time vt
                                            :transact-time tt})
              results (api/q db query)]
          {:status 200
           :body (cond
                   html? (let [links (link-top-level-entities db  "/_crux/entity" results)]
                           (raw-html
                            {:body (query->html links query results)
                             :title "/query"
                             :options options
                             :results {:query-results
                                       {"linked-entities" links
                                        "query-results" results}}}))
                   csv? (with-out-str
                          (csv/write-csv *out* results))
                   tsv? (with-out-str
                          (csv/write-csv *out* results :separator \tab))
                   link-entities? {"linked-entities" (link-top-level-entities db  "/_crux/entity" results)
                                   "query-results" results}
                   :else results)})
        (catch Exception e
          {:status 400
           :body (if html?
                   (raw-html
                    {:title "/query"
                     :body
                     [:div.error-box (.getMessage e)]})
                   (with-out-str
                     (pp/pprint (Throwable->map e))))})))))

(defn- data-browser-handler [crux-node options request]
  (condp check-path request
    [#"^/_crux/index$" [:get]]
    (root-handler crux-node options request)

    [#"^/_crux/index.html$" [:get]]
    (root-handler crux-node options (assoc-in request [:muuntaja/response :format] "text/html"))

    [#"^/_crux/status" [:get]]
    (status crux-node options request)

    [#"^/_crux/entity$" [:get]]
    (entity-state crux-node options request)

    [#"^/_crux/query$" [:get]]
    (data-browser-query crux-node options request)

    [#"^/_crux/query.csv$" [:get]]
    (data-browser-query crux-node options (assoc-in request [:muuntaja/response :format] "text/csv"))

    [#"^/_crux/query.tsv$" [:get]]
    (data-browser-query crux-node options (assoc-in request [:muuntaja/response :format] "text/tsv"))

    nil))

(def ^:const default-server-port 3000)

(defrecord HTTPServer [^Server server options]
  Closeable
  (close [_]
    (.stop server)))

(defn- unsupported-encoder [_]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data charset]
      (throw (UnsupportedOperationException.)))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ data charset]
      (throw (UnsupportedOperationException.)))))

(defn valid-jwt?
  "Return true if the given JWS is valid with respect to the given
  signing key."
  [^String jwt ^JWKSet jwks]
  (try
    (let [jws (SignedJWT/parse ^String jwt)
          kid (.. jws getHeader getKeyID)
          jwk (.getKeyByKeyId jwks kid)
          verifier (case (.getValue ^KeyType (.getKeyType jwk))
                     "RSA" (RSASSAVerifier. ^RSAKey jwk)
                     "EC"  (ECDSAVerifier. ^ECKey jwk))]
      (.verify jws verifier))
    (catch Exception e
      false)))

(defn wrap-jwt [handler jwks]
  (fn [request]
    (if-not (valid-jwt? (or (get-in request [:headers "x-amzn-oidc-accesstoken"])
                            (some->> (get-in request [:headers "authorization"])
                                     (re-matches #"Bearer (.*)")
                                     (second)))
                        jwks)
      {:status 401
       :body "JWT Failed to validate"}

      (handler request))))

(def muuntaja-handler
  (-> m/default-options
      (assoc-in [:formats "text/html"] (mfc/map->Format {:name "text/html"
                                                         :encoder [unsupported-encoder]}))
      (assoc-in [:formats "text/csv"] (mfc/map->Format {:name "text/csv"
                                                        :encoder [unsupported-encoder]}))
      (assoc-in [:formats "text/tsv"] (mfc/map->Format {:name "text/tsv"
                                                        :encoder [unsupported-encoder]}))))

(def module
  {::server {:start-fn (fn [{:keys [crux.node/node]} {::keys [port read-only? ^String jwks] :as options}]
                         (let [server (j/run-jetty
                                       (-> (some-fn (-> #(handler % {:crux-node node, ::read-only? read-only?})
                                                        (p/wrap-params)
                                                        (wrap-exception-handling))
                                                    (-> (partial data-browser-handler node options)
                                                        (p/wrap-params)
                                                        (wrap-resource "public")
                                                        (wrap-format muuntaja-handler)
                                                        (wrap-exception-handling))

                                                    (fn [request]
                                                      {:status 404
                                                       :headers {"Content-Type" "text/plain"}
                                                       :body "Could not find resource."}))
                                           (cond-> jwks (wrap-jwt (JWKSet/parse jwks))))
                                       {:port port
                                        :join? false})]
                           (log/info "HTTP server started on port: " port)
                           (->HTTPServer server options)))
             :deps #{:crux.node/node}

             ;; I'm deliberately not porting across CORS here as I don't think we should be encouraging
             ;; Crux servers to be exposed directly to browsers. Better pattern here for individual apps
             ;; to expose the functionality they need to? (JH)
             :args {::port {:crux.config/type :crux.config/nat-int
                            :doc "Port to start the HTTP server on"
                            :default default-server-port}
                    ::read-only? {:crux.config/type :crux.config/boolean
                                  :doc "Whether to start the Crux HTTP server in read-only mode"
                                  :default false}
                    ::jwks {:crux.config/type :crux.config/string
                            :doc "JWKS string to validate against"}}}})
