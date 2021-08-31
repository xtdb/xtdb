(ns xtdb.ui.subscriptions
  (:require
   [clojure.string :as string]
   [cljs.reader :as reader]
   [xtdb.ui.common :as common]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]
   [clojure.data :as data]
   [xtdb.http-server.entity-ref :as entity-ref :refer [EntityRef]]))

(rf/reg-sub
 :db
 (fn [db _] db))

(rf/reg-sub
 ::current-route
 (fn [db _]
   (:current-route db)))

(def query-root-str
  (string/join "\n"
               [";; Welcome to the XTDB Console!"
                ";;"
                ";; The Console is a tool for exploring your data and helping you write"
                ";; Datalog queries. The console also displays useful status information."
                ";;"
                ";; See the documentation for help on how to construct queries."
                ";;"
                ";; Keyboard Shortcuts:"
                ";; Ctrl+Enter (Submit Query)"
                ""
                "{"
                " :find [?e]"
                " :where [[?e :xt/id]]"
                "}"]))

(rf/reg-sub
 ::initial-values-query
 (fn [db _]
   (let [{:keys [valid-time tx-time query-edn]} (get-in db [:current-route :query-params])
         query-string (some-> (try
                                (reader/read-string query-edn)
                                (catch js/Error _ nil))
                              common/query->formatted-query-string)
         valid-time (str (or valid-time (t/now)))
         latest-tx-time (some-> (get-in db [:options :latest-completed-tx])
                                (::xt/tx-time)
                                (t/instant))
         tx-time (str (or tx-time latest-tx-time))]
     {"q" (or query-string query-root-str)
      "valid-time" (js/moment valid-time)
      "tx-time" (js/moment tx-time)})))

(rf/reg-sub
 ::valid-time
 (fn [db _]
   (get-in db [:current-route :query-params :valid-time] (t/now))))

(rf/reg-sub
 ::tx-time
 (fn [db _]
   (let [latest-tx-time (some-> (get-in db [:options :latest-completed-tx])
                                (::xt/tx-time)
                                (t/instant))]
     (get-in db [:current-route :query-params :tx-time] latest-tx-time))))

(rf/reg-sub
 ::initial-values-entity
 (fn [db _]
   (let [query-params (get-in db [:current-route :query-params])
         valid-time (str (:valid-time query-params (t/now)))
         latest-tx-time (some-> (get-in db [:options :latest-completed-tx])
                                (::xt/tx-time)
                                (t/instant))
         tx-time (str (:tx-time query-params latest-tx-time))]
     {"eid" (:eid-edn query-params)
      "valid-time" (js/moment valid-time)
      "tx-time" (js/moment tx-time)})))

;; wrap this in reg-sub-raw and replace get-in with subs
(rf/reg-sub
 ::query-data-table
 (fn [db _]
   (if-let [error (get-in db [:query :error])]
     {:error error}
     (let [query-results (get-in db [:query :http])
           query (reader/read-string (get-in db [:current-route :query-params :query-edn]))
           find-clause (:find query)
           table-loading? (get-in db [:query :result-pane :loading?])
           latest-tx-time (some-> (get-in db [:options :latest-completed-tx])
                                  (::xt/tx-time)
                                  (t/instant))
           time-info {:valid-time (get-in db [:current-route :query-params :valid-time] (t/now))
                      :tx-time (get-in db [:current-route :query-params :tx-time] latest-tx-time)}
           columns (map (fn [column]
                          {:column-key column
                           :column-name (str column)
                           :render-fn
                           (fn [_ v]
                             (if (instance? EntityRef v)
                               [:a {:href (entity-ref/EntityRef->url v time-info)} (str (:eid v))]
                               (str v)))
                           :render-only #{:filter :sort}})
                        find-clause)
           rows (when query-results
                  (map #(zipmap find-clause %) query-results))]
       {:data
        {:columns columns
         :rows rows
         :offset (or (:offset query) 0)
         :loading? (or (nil? table-loading?) table-loading?)
         :filters {:input (into #{} find-clause)}}}))))

(rf/reg-sub
 ::request-times
 (fn [db _]
   (let [now (t/now)]
     [(get-in db [:request :start-time] now)
      (get-in db [:request :end-time] now)])))

(rf/reg-sub
 ::query-limit
 (fn [db _]
   (js/parseInt (get-in db [:current-route :query-params :limit] 100))))

(rf/reg-sub
 ::query-offset
 (fn [db _]
   (js/parseInt (get-in db [:current-route :query-params :offset] 0))))

(rf/reg-sub
 ::query-result-pane-loading?
 (fn [db _]
   (get-in db [:query :result-pane :loading?])))

(rf/reg-sub
 ::entity-result-pane-loading?
 (fn [db _]
   (get-in db [:entity :result-pane :loading?])))

(rf/reg-sub
 ::eid-submitted?
 (fn [db _]
   (some? (get-in db [:current-route :query-params :eid-edn]))))

(rf/reg-sub
 ::entity-pane-tab
 (fn [db _]
   (if (get-in db [:current-route :query-params :history])
     :history
     :document)))

(rf/reg-sub
 ::console-tab
 (fn [db _]
   (get-in db [:current-route :data :name] :query)))

(rf/reg-sub
 ::query-form-tab
 (fn [db _]
   (get-in db [:query-form :selected-tab] :edit-query)))

(rf/reg-sub
 ::status-tab
 (fn [db _]
   (get-in db [:status :selected-tab] :overview)))

(rf/reg-sub
 ::query-submitted?
 (fn [db _]
   (not-empty (get-in db [:current-route :query-params]))))

(rf/reg-sub
 ::query-data-download-link
 (fn [db [_ link-type]]
   (let [query-params (get-in db [:current-route :query-params])]
     (-> (common/route->url :query {} query-params)
         (string/replace-first #"query" (str "query." link-type))))))

(rf/reg-sub
 ::entity-result-pane-document
 (fn [db _]
   (if-let [error (get-in db [:entity :error])]
     {:error error}
     (let [query-params (get-in db [:current-route :query-params])]
       {:eid (:eid-edn query-params)
        :vt (common/iso-format-datetime (or (:valid-time query-params) (t/now)))
        :tt (or (common/iso-format-datetime (:tx-time query-params)) "Using Latest")
        :document (get-in db [:entity :http :document])}))))

(rf/reg-sub
 ::entity-result-pane-document-error
 (fn [db _]
   (get-in db [:entity :error])))


(defn- format-history-times [entity-history]
  (map
   (fn [history-element]
     (-> history-element
         (update ::xt/tx-time common/iso-format-datetime)
         (update ::xt/valid-time common/iso-format-datetime)))
   entity-history))

(rf/reg-sub
 ::entity-result-pane-history
 (fn [db _]
   (let [eid (get-in db [:current-route :query-params :eid-edn])
         history (-> (get-in db [:entity :http :history])
                     format-history-times)]
     {:eid eid
      :entity-history history})))

(defn- history-docs->diffs [entity-history]
  (map
   (fn [[x y]]
     (let [[deletions additions]
           (data/diff (::xt/doc x) (::xt/doc y))]
       (merge
        (select-keys y [::xt/tx-time ::xt/valid-time])
        {:deletions deletions
         :additions additions})))
   (partition 2 1 entity-history)))

(rf/reg-sub
 ::entity-result-pane-history-diffs
 (fn [db _]
   (let [eid (get-in db [:current-route :query-params :eid-edn])
         history (-> (get-in db [:entity :http :history])
                     format-history-times)
         entity-history (history-docs->diffs history)]
     {:eid eid
      :up-to-date-doc (first history)
      :history-diffs entity-history})))

(rf/reg-sub
 ::form-pane-history
 (fn [db [_ component]]
   (get-in db [:form-pane :history component])))

(rf/reg-sub
 ::query-form-history
 (fn [db _]
   ;; Get newest first
   (mapv
    (fn [x]
      {"q" (common/query->formatted-query-string (:query-edn x))})
    (:query-history db))))

(rf/reg-sub
 ::show-vt?
 (fn [db [_ component]]
   (let [url-has-vt? (contains? (get-in db [:current-route :query-params]) :valid-time)]
     (get-in db [:form-pane :show-vt? component] url-has-vt?))))

(rf/reg-sub
 ::show-tt?
 (fn [db [_ component]]
   (let [url-has-tt? (contains? (get-in db [:current-route :query-params]) :tx-time)]
     (get-in db [:form-pane :show-tt? component] url-has-tt?))))

(rf/reg-sub
 ::entity-form-history
 (fn [db _]
   (:entity-history db)))

(rf/reg-sub
 ::query-form-visible?
 (fn [db _]
   (get-in db [:query-form :visible?] true)))

(rf/reg-sub
 ::form-pane-hidden?
 (fn [db _]
   (get-in db [:form-pane :hidden?] false)))

(rf/reg-sub
 ::node-status-loading?
 (fn [db _]
   (get-in db [:status :loading?])))

(rf/reg-sub
 ::node-status
 (fn [db _]
   (get-in db [:status :http])))

(rf/reg-sub
 ::node-attribute-stats
 (fn [db _]
   (get-in db [:attribute-stats :http])))

(rf/reg-sub
 ::node-attribute-stats-loading?
 (fn [db _]
   (get-in db [:attribute-stats :loading?])))

(rf/reg-sub
 ::node-options
 (fn [db _]
   (:node-options (:options db))))
