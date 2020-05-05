(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-sub
 :db
 (fn [db _]
   db))

(rf/reg-sub
 ::current-page
 (fn [db _]
   (:current-page db)))

(rf/reg-sub
 ::query-data-table
 (fn [db _]
   (if-let [error (get-in db [:query-data :error])]
     {:error error}
     (let [{:strs [query-results find-clause linked-entities]}
           (:query-data db)
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
         :loading? (not query-results)
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
