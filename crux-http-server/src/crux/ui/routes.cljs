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
     [{:start (fn [& params] (js/console.log "Entering sub-page 1"))
       :stop (fn [& params] (js/console.log "Leaving sub-page 1"))}]}]
   ["/_entity/:eid"
    {:name :entity
     :link-text "Entity"
     :controllers
     [;; events are triggered when leaving the view or when path-params or
      ;; query-params change
      {:identity #(do [(get-in % [:path-params :eid])
                       (get-in % [:query-params])
                       ;; TBD we might want to always fetch even if url doesn't
                       ;; change. Discuss this on Monday.
                       (gensym)])
       :start #(rf/dispatch [:crux.ui.http/fetch-entity])
       :stop (fn [& params] (js/console.log "Leaving sub-page 2"))}]}]])
