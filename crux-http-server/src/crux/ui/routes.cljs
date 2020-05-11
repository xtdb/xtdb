(ns crux.ui.routes
  (:require
   [re-frame.core :as rf]))

(def routes
  [""
   ["/"
    {:name :homepage
     :link-text "Home"
     :controllers
     [{:start (fn [& params] (js/console.log "Entering home page"))
       :stop (fn [& params] (js/console.log "Leaving home page"))}]}]
   ["/_query"
    {:name :query
     :link-text "Query"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-query-table])
       :stop (fn [& params] (js/console.log "Leaving sub-page 1"))}]}]
   ["/_entity/:eid"
    {:name :entity
     :link-text "Entity"
     :controllers
     [{:identity #(gensym)
       :start #(rf/dispatch [:crux.ui.http/fetch-entity])
       :stop (fn [& params] (js/console.log "Leaving sub-page 2"))}]}]])
