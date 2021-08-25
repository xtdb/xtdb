(ns crux.ui.routes
  (:require
   [re-frame.core :as rf]))

(def routes
  [""
   ["/_crux/attribute-stats"
    {:name :attribute-stats}]
   ["/_crux/status"
    {:name :status
     :link-text "Status"
     :controllers
     [{:start #(do
                 (rf/dispatch [:crux.ui.http/fetch-node-status])
                 (rf/dispatch [:crux.ui.http/fetch-node-attribute-stats]))}]}]
   ["/_crux/query"
    {:name :query
     :link-text "Query"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-query-table])}]}]
   ["/_crux/entity"
    {:name :entity
     :link-text "Entity"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-entity])}]}]])
