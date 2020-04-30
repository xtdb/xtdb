(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]))

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
   (let [{:strs [query-results find-clause linked-entities]}
         (:query-data db)
         columns (map (fn [column]
                        {:column-key column
                         :column-name (str column)
                         :render-fn
                         (fn [_ v]
                           (if-let [link (get linked-entities v)]
                             [:a {:href link} v]
                             v))
                         :render-only #{:filter :sort}})
                      find-clause)
         rows (map #(zipmap find-clause %) query-results)]
     {:columns columns
      :rows rows
      :loading? false
      :filters {:input (into #{} find-clause)}})))

(defn set-query-param
  [q-param value]
  (->
   (js/URL. js/window.location.href)
   (.-search)
   (js/URLSearchParams.)
   (doto (.set q-param value))))

(rf/reg-sub
 ::prev-next-query-params
 (fn [db _]
   (let [results (get-in db [:query-data "query-results"])
         limit (->> (or (.get (js/URLSearchParams. js/window.location.search) "limit")
                        (get-in db [:options :crux.http-server/query-result-page-limit]))
                    (js/parseInt))
         offset (->> (or (.get (js/URLSearchParams. js/window.location.search) "offset") "0")
                     (js/parseInt))
         prev-offset (when-not (zero? offset)
                       (max 0 (- offset limit)))
         next-offset (when (= limit (count results))
                       (+ offset limit))]
     {:prev-query-params (when prev-offset (set-query-param "offset" prev-offset))
      :next-query-params (when next-offset (set-query-param "offset" next-offset))})))

(rf/reg-sub
 ::entity-view-data
 (fn [db _]
   {:entity-result (get-in db [:entity-data "entity"])
    :linked-entities (get-in db [:entity-data "linked-entities"])}))
