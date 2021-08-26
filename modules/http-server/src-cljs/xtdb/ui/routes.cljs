(ns xtdb.ui.routes
  (:require
   [re-frame.core :as rf]))

(def routes
  [""
   ["/_xtdb/attribute-stats"
    {:name :attribute-stats}]
   ["/_xtdb/status"
    {:name :status
     :link-text "Status"
     :controllers
     [{:start #(do
                 (rf/dispatch [:xtdb.ui.http/fetch-node-status])
                 (rf/dispatch [:xtdb.ui.http/fetch-node-attribute-stats]))}]}]
   ["/_xtdb/query"
    {:name :query
     :link-text "Query"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:xtdb.ui.http/fetch-query-table])}]}]
   ["/_xtdb/entity"
    {:name :entity
     :link-text "Entity"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:xtdb.ui.http/fetch-entity])}]}]])
