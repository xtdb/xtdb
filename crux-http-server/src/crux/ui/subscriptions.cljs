(ns crux.ui.subscriptions
  (:require
   [cljs.reader :as reader]
   [clojure.pprint :as p]
   [crux.ui.common :as common]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-sub
 :db
 (fn [db _] db))

(rf/reg-sub
 ::current-route
 (fn [db _]
   (:current-route db)))

(rf/reg-sub
 ::initial-values-query
 (fn [db _]
   (let [query-params (get-in db [:current-route :query-params])
         handler (get-in db [:current-route :data :name])
         valid-time (common/datetime->date-time
                     (str (:valid-time query-params (t/now))))
         transaction-time (common/datetime->date-time
                           (:transaction-time query-params))]
     (when (= :query handler)
       {"q" (common/query-params->formatted-edn-string
             (dissoc query-params :valid-time :transaction-time))
        "vtd" (:date valid-time)
        "vtt" (:time valid-time)
        "ttd" (:date transaction-time)
        "ttt" (:time transaction-time)}))))

(rf/reg-sub
 ::initial-values-entity
 (fn [db _]
   (let [query-params (get-in db [:current-route :query-params])
         handler (get-in db [:current-route :data :name])
         valid-time (common/datetime->date-time
                     (str (:valid-time query-params (t/now))))
         transaction-time (common/datetime->date-time
                           (:transaction-time query-params))]
     (when (= :entity handler)
       {"eid" (get-in db [:current-route :path-params :eid])
        "vtd" (:date valid-time)
        "vtt" (:time valid-time)
        "ttd" (:date transaction-time)
        "ttt" (:time transaction-time)}))))

;; wrap this in reg-sub-raw and replace get-in with subs
(rf/reg-sub
 ::query-data-table
 (fn [db _]
   (if-let [error (get-in db [:query :error])]
     {:error error}
     (let [{:strs [query-results linked-entities]}
           (get-in db [:query :http])
           find-clause (reader/read-string (get-in db [:current-route :query-params :find]))
           table-loading? (get-in db [:query :right-pane :loading?])
           offset (->> (or (get-in db [:current-route :query-params :offset]) "0")
                       (js/parseInt))
           columns (map (fn [column]
                          {:column-key column
                           :column-name (str column)
                           :render-fn
                           (fn [_ v]
                             (if-let [link (get linked-entities v)]
                               [:a.entity-link {:href link}
                                (str v)]
                               v))
                           :render-only #{:filter :sort}})
                        find-clause)
           rows (map #(zipmap find-clause %) query-results)]
       {:data
        {:columns columns
         :rows rows
         :offset offset
         :loading? table-loading?
         :filters {:input (into #{} find-clause)}}}))))

(rf/reg-sub
 ::query-right-pane-view
 (fn [db _]
   (or (get-in db [:query :right-pane :view]) :table)))

(rf/reg-sub
 ::query-right-pane-loading?
 (fn [db _]
   (get-in db [:query :right-pane :loading?])))

(rf/reg-sub
 ::entity-right-pane-loading?
 (fn [db _]
   (get-in db [:entity :right-pane :loading?])))

(rf/reg-sub
 ::entity-right-pane-view
 (fn [db _]
   (if (get-in db [:current-route :query-params :history]) :history :document)))

(rf/reg-sub
 ::entity-right-pane-document
 (fn [db _]
   (if-let [error (get-in db [:entity :error])]
     {:error error}
     (let [query-params (get-in db [:current-route :query-params])
           document (get-in db [:entity :http "entity"])]
       {:eid (get-in db [:current-route :path-params :eid])
        :vt (or (:valid-time query-params) (str (t/now)))
        :tt (or (:transaction-time query-params) "Not Specified")
        :document document
        :document-no-eid (dissoc document :crux.db/id)
        :linked-entities (get-in db [:entity :http "linked-entities"])}))))

(rf/reg-sub
 ::entity-right-pane-history
 (fn [db _]
   {:eid (get-in db [:current-route :path-params :eid])
    :entity-history (get-in db [:entity :http])}))

(rf/reg-sub
 ::left-pane-view
 (fn [db _]
   (or (get-in db [:left-pane :view])
       (get-in db [:current-route :data :name]))))

(rf/reg-sub
 ::left-pane-visible?
 (fn [db _]
   (get-in db [:left-pane :visible?])))
