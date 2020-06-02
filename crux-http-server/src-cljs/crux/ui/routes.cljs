(ns crux.ui.routes
  (:require
   [re-frame.core :as rf]))

(def routes
  [""
   ["/"
    {:name :homepage
     :link-text "Home"
     :controllers
     [{:start (fn [& params])
       :stop (fn [& params])}]}]
   ["/_crux/query"
    {:name :query
     :link-text "Query"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-query-table])
       :stop (fn [& params])}]}]
   ["/_crux/entity/:eid"
    {:name :entity
     :link-text "Entity"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-entity])
       :stop (fn [& params])}]}]])
