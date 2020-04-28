(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]))

(rf/reg-sub
 ::metadata
 (fn [db _]
   (let [{:strs [query-results find-clause linked-entities]}
         (:metadata db)
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
