(ns crux.http-server
  "HTTP API for Crux.

  The optional SPARQL handler requires juxt.crux/rdf."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.instant :as instant]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.tx :as tx]
            [ring.adapter.jetty :as j]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.resource :refer [wrap-resource]]
            [muuntaja.middleware :refer [wrap-format]]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [ring.middleware.params :as p]
            [ring.util.io :as rio]
            [ring.util.request :as req]
            [ring.util.time :as rt]
            [hiccup.page :as page]
            [hiccup2.core :as hiccup2]
            [crux.api :as api])
  (:import [crux.api ICruxAPI ICruxDatasource NodeOutOfSyncException]
           [java.io Closeable IOException OutputStream]
           java.time.Duration
           java.util.Date
           java.net.URLDecoder
           org.eclipse.jetty.server.Server))

;; ---------------------------------------------------
;; Utils

(defn- raw-html
  [{:keys [body title options]}]
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
          [:link {:rel "stylesheet" :href "/css/all.css"}]
          [:link {:rel "stylesheet" :href "/css/table.css"}]
          [:link {:rel "stylesheet"
                  :href "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.12.1/css/all.min.css"}]
          [:title "Crux Console"]]
         [:body
          [:nav.console-nav
           [:div "Crux Console"]]
          [:div.console
           [:div#app
            [:h1 title]
            body]]
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

(defn- status [^ICruxAPI crux-node]
  (let [status-map (.status crux-node)]
    (if (or (not (contains? status-map :crux.zk/zk-active?))
            (:crux.zk/zk-active? status-map))
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (cio/pr-edn-str status-map)))))

(defn- document [^ICruxAPI crux-node request]
  (let [[_ content-hash] (re-find #"^/document/(.+)$" (req/path-info request))]
    (success-response
     (.document crux-node (c/new-id content-hash)))))

(defn- stringify-keys [m]
  (persistent!
    (reduce-kv
      (fn [m k v] (assoc! m (str k) v))
      (transient {})
      m)))

(defn- documents [^ICruxAPI crux-node  {:keys [query-params] :as request}]
  ; TODO support for GET
  (let [preserve-ids? (Boolean/parseBoolean (get query-params "preserve-crux-ids" "false"))
        content-hashes-set (body->edn request)
        ids-set (set (map c/new-id content-hashes-set))]
    (success-response
      (cond-> (.documents crux-node ids-set)
              (not preserve-ids?) stringify-keys))))

(defn- history [^ICruxAPI crux-node request]
  (let [[_ eid] (re-find #"^/history/(.+)$" (req/path-info request))
        history (.history crux-node (c/new-id (URLDecoder/decode eid)))]
    (-> (success-response history)
        (add-last-modified (:crux.tx/tx-time (first history))))))

(defn- parse-history-range-params [{:keys [query-params] :as request}]
  (let [[_ eid] (re-find #"^/history-range/(.+)$" (req/path-info request))
        times (map #(some-> (get query-params %) not-empty cio/parse-rfc3339-or-millis-date)
                   ["valid-time-start" "transaction-time-start" "valid-time-end" "transaction-time-end"])]
    (cons eid times)))

(defn- history-range [^ICruxAPI crux-node request]
  (try
    (let [[eid valid-time-start transaction-time-start valid-time-end transaction-time-end] (parse-history-range-params request)
          history (.historyRange crux-node (c/new-id (URLDecoder/decode eid)) valid-time-start transaction-time-start valid-time-end transaction-time-end)
          last-modified (:crux.tx/tx-time (last history))]
      (-> (success-response history)
          (add-last-modified (:crux.tx/tx-time (last history)))))
    (catch NodeOutOfSyncException e
      (exception-response 400 e))))

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

;; TODO: Potentially require both valid and transaction time sent by
;; the client?
(defn- query [^ICruxAPI crux-node request]
  (let [query-map (doto (body->edn request) (validate-or-throw ::query-map))
        db (db-for-request crux-node query-map)]
    (-> (success-response
         (.query db (:query query-map)))
        (add-last-modified (.transactionTime db)))))

(defn- query-stream [^ICruxAPI crux-node request]
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

(defn- history-ascending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (doto (body->edn request) (validate-or-throw ::entity-map))
        db (db-for-request crux-node body)
        history (api/open-history-ascending db (c/new-id eid))]
    (-> (streamed-edn-response history (iterator-seq history))
        (add-last-modified (:crux.tx/tx-time (.latestCompletedTx crux-node))))))

(defn- history-descending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (doto (body->edn request) (validate-or-throw ::entity-map))
        db (db-for-request crux-node body)
        history (api/open-history-descending db (c/new-id eid))]
    (-> (streamed-edn-response history (iterator-seq history))
        (add-last-modified (:crux.tx/tx-time (.latestCompletedTx crux-node))))))

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

(defn- handler [crux-node request]
  (condp check-path request
    [#"^/$" [:get]]
    (status crux-node)

    [#"^/document/.+$" [:get :post]]
    (document crux-node request)

    [#"^/documents" [:post]]
    (documents crux-node request)

    [#"^/entity/.+$" [:get]]
    (entity crux-node request)

    [#"^/entity-tx/.+$" [:get]]
    (entity-tx crux-node request)

    [#"^/entity$" [:post]]
    (entity crux-node request)

    [#"^/entity-tx$" [:post]]
    (entity-tx crux-node request)

    [#"^/history/.+$" [:get :post]]
    (history crux-node request)

    [#"^/history-range/.+$" [:get]]
    (history-range crux-node request)

    [#"^/history-ascending$" [:post]]
    (history-ascending crux-node request)

    [#"^/history-descending$" [:post]]
    (history-descending crux-node request)

    [#"^/entity-history/.+$" [:get]]
    (entity-history crux-node request)

    [#"^/query$" [:post]]
    (query crux-node request)

    [#"^/query-stream$" [:post]]
    (query-stream crux-node request)

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
    (transact crux-node request)

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

(defn link-all-entities
  [db path result]
  (letfn [(recur-on-result [result links]
             (if (and (c/valid-id? result)
                      (api/entity db result))
               (let [query-params (format "?valid-time=%s&transaction-time=%s"
                                          (.toInstant ^Date (api/valid-time db))
                                          (.toInstant ^Date (api/transaction-time db)))]
                 (assoc links result (str path "/" result query-params)))
               (cond
                 (map? result) (apply merge (map #(recur-on-result % links) (vals result)))
                 (sequential? result) (apply merge (map #(recur-on-result % links) result))
                 :else links)))]
    (recur-on-result result {})))

(defn- entity->html [eid linked-entities entity-map vt tt]
  (let [nodes (fn resolve-entity-map
                [linked-entities entity-map]
                (if-let [href (get linked-entities entity-map)]
                  [:a.entity-link
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
                    :else (str entity-map))))]
    [:div.entity-map__container
     (if entity-map
       [:div.entity-map
        [:div.entity-group
         [:div.entity-group__key
          ":crux.db/id"]
         [:div.entity-group__value
          (str (:crux.db/id entity-map))]]
        [:hr.entity-group__separator]
        (nodes (linked-entities) (dissoc entity-map :crux.db/id))]
       [:div.enttiy-map
        [:strong (str eid)] " entity not found"])
      [:div.entity-vt-tt
       [:div.entity-vt-tt__title
        "Valid Time"]
       [:div.entity-vt-tt__value
        (str (or vt (java.util.Date.)))]
       [:div.entity-vt-tt__title
        "Transaction Time"]
       [:div.entity-vt-tt__value
        (str (or tt "Not Specified"))]]]))

(defn- entity-state [^ICruxAPI crux-node options request]
  (let [[_ encoded-eid] (re-find #"^/_entity/(.+)$" (req/path-info request))
        eid (c/id-edn-reader (URLDecoder/decode encoded-eid))
        query-params (:query-params request)
        vt (some-> (get query-params "valid-time")
                   (instant/read-instant-date))
        tt (some-> (get query-params "transaction-time")
                   (instant/read-instant-date))
        db (db-for-request crux-node {:valid-time vt
                                      :transact-time tt})
        entity-map (api/entity db eid)
        linked-entities #(link-all-entities db  "/_entity" entity-map)]
    {:status (if (some? entity-map) 200 404)
     :body (cond
             (= (get-in request [:muuntaja/response :format]) "text/html")
             (raw-html
              {:body (entity->html encoded-eid linked-entities entity-map vt tt)
               :title "/_entity"
               :options options})

             (get query-params "link-entities?")
             {"linked-entities" (linked-entities)
              "entity" entity-map}

             :else entity-map)}))

(defn- vectorize-param [param]
  (if (vector? param) param [param]))

(defn- build-query [{:strs [find where args order-by limit offset full-results]}]
  (let [new-offset (if offset
                     (Integer/parseInt offset)
                     0)]
    (cond-> {:find (c/read-edn-string-with-readers find)
             :where (->> where vectorize-param (mapv c/read-edn-string-with-readers))
             :offset new-offset}
      args (assoc :args (c/read-edn-string-with-readers args))
      order-by (assoc :order-by (->> order-by vectorize-param (mapv c/read-edn-string-with-readers)))
      limit (assoc :limit (Integer/parseInt limit))
      full-results (assoc :full-results? true))))

(defn link-top-level-entities
  [db path results]
  (->> (apply concat results)
       (filter (every-pred c/valid-id? #(api/entity db %)))
       (map (fn [id]
              (let [query-params (format "?valid-time=%s&transaction-time=%s"
                                         (.toInstant ^Date (api/valid-time db))
                                         (.toInstant ^Date (api/transaction-time db)))]
                [id (str path "/" id query-params)])))
       (into {})))

(defn resolve-prev-next-offset
  [query-params prev-offset next-offset]
  (let [url (str "/_query?"
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
                 [:a.entity-link {:href href} (str cell-value)]
                 (str cell-value))])])]
        [:tbody.table__body.table__no-data
         [:tr [:td.td__no-data
               "Nothing to show"]]])]]
    [:table.table__foot]]])

(defn data-browser-query [^ICruxAPI crux-node options request]
  (let [query-params (:query-params request)
        link-entities? (get query-params "link-entities?")
        html? (= (get-in request [:muuntaja/response :format]) "text/html")]
    (cond
      (empty? query-params)
      (if html?
        {:status 200
         :body (raw-html
                {:body [:form
                        {:action "/_query"}
                        [:textarea.textarea
                         {:name "q"
                          :cols 40
                          :rows 10}]
                        [:br]
                        [:br]
                        [:button.button
                         {:type "submit"}
                         "Submit Query"]]
                 :title "/_query"
                 :options options})}
        {:status 400
         :body "No query provided."})

      :else
      (try
        (let [query (cond-> (or (some-> (get query-params "q")
                                        (edn/read-string))
                                (build-query query-params))
                      html? (dissoc :full-results?))
              db (db-for-request crux-node {:valid-time (some-> (get query-params "valid-time")
                                                                (instant/read-instant-date))
                                            :transact-time (some-> (get query-params "transaction-time")
                                                                   (instant/read-instant-date))})
              results (api/q db query)]
          {:status 200
           :body (cond
                   html? (let [links (if link-entities? (link-top-level-entities db  "/_entity" results) [])]
                           (raw-html
                            {:body (query->html links query results)
                             :title "/_query"
                             :options options}))
                   link-entities? {"linked-entities" (link-top-level-entities db  "/_entity" results)
                                   "query-results" results}
                   :else results)})
        (catch Exception e
          {:status 400
           :body (if html?
                   (raw-html
                    {:title "/_query"
                     :body
                     [:div.error-box (.getMessage e)]})
                   (with-out-str
                     (pp/pprint (Throwable->map e))))})))))

(defn- data-browser-handler [crux-node options request]
  (condp check-path request
    [#"^/_entity/.+$" [:get]]
    (entity-state crux-node options request)

    [#"^/_query" [:get]]
    (data-browser-query crux-node options request)

    nil))

(def ^:const default-server-port 3000)

(defrecord HTTPServer [^Server server options]
  Closeable
  (close [_]
    (.stop server)))

(defn ^:deprecated start-http-server
  "Starts a HTTP server listening to the specified server-port, serving
  the Crux HTTP API. Takes a either a crux.api.ICruxAPI or its
  dependencies explicitly as arguments (internal use)."
  ^java.io.Closeable
  ([crux-node] (start-http-server crux-node {}))
  ([crux-node
    {:keys [server-port cors-access-control]
     :or {server-port default-server-port cors-access-control []}
     :as options}]
   (let [wrap-cors' #(apply wrap-cors (cons % cors-access-control))
         server (j/run-jetty (-> (partial handler crux-node)
                                 (wrap-exception-handling)
                                 (p/wrap-params)
                                 (wrap-cors')
                                 (wrap-exception-handling))
                             {:port server-port
                              :join? false})]
     (log/info "HTTP server started on port: " server-port)
     (->HTTPServer server options))))

(defn- html-encoder [_]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data charset]
      (throw (UnsupportedOperationException.)))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ data charset]
      (throw (UnsupportedOperationException.)))))

(def module
  {::server {:start-fn (fn [{:keys [crux.node/node]} {::keys [port] :as options}]
                         (let [server (j/run-jetty (some-fn (-> (partial handler node)
                                                                (p/wrap-params)
                                                                (wrap-exception-handling))

                                                            (-> (partial data-browser-handler node options)
                                                                (p/wrap-params)
                                                                (wrap-resource "public")
                                                                (wrap-format (assoc-in m/default-options
                                                                                       [:formats "text/html"]
                                                                                       (mfc/map->Format {:name "text/html"
                                                                                                         :encoder [html-encoder]})))
                                                                (wrap-exception-handling))

                                                            (fn [request]
                                                              {:status 404
                                                               :headers {"Content-Type" "text/plain"}
                                                               :body "Could not find resource."}))
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
                            :default default-server-port}}}})
