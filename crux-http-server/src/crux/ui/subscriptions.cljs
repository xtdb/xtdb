(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-sub
 :db
 (fn [db _]
   db))

(rf/reg-sub
 ::current-route
 (fn [db _]
   (:current-route db)))

(rf/reg-sub
 ::query-data-table
 (fn [db _]
   (if-let [error (get-in db [:query-data :error])]
     {:error error}
     (let [{:strs [query-results find-clause linked-entities]}
           (:query-data db)
           table-loading? (:table-loading? db)
           offset (->> (or (.get (js/URLSearchParams. js/window.location.search) "offset") "0")
                       (js/parseInt))
           columns (map (fn [column]
                          {:column-key column
                           :column-name (str column)
                           :render-fn
                           (fn [_ v]
                             (if-let [link (get linked-entities v)]
                               [:a.entity-link {:href link} v]
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
 ::entity-view-data
 (fn [db _]
   (let [get-param #(.get (js/URLSearchParams. js/window.location.search) %)]
     {:vt (or (get-param "valid-time") (str (t/now)))
      :tt (or (get-param "transaction-time") "Not Specified")
      :entity-name (get-in db [:current-page :route-params :entity-id])
      :entity-result (get-in db [:entity-data "entity"])
      :linked-entities (get-in db [:entity-data "linked-entities"])})))

(rf/reg-sub
 ::entity-loading?
 (fn [db _]
   (:entity-loading? db)))

(rf/reg-sub
 ::query-pane-show?
 (fn [db _]
   (:query-pane-show? db)))

(rf/reg-sub
 ::query-view
 (fn [db _]
   (get db :query-view :table)))

(rf/reg-sub
 ::entity-view
 (fn [db _]
   (get db :entity-view :document)))

(rf/reg-sub
 ::search-view
 (fn [db _]
   (get db :search-view :query)))
