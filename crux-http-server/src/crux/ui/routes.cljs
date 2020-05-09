(ns crux.ui.routes)

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
   ["/_entity/:entity-id"
    {:name :entity
     :link-text "Entity"
     :controllers
     [{:start (fn [& params] (js/console.log "Entering sub-page 2"))
       :stop (fn [& params] (js/console.log "Leaving sub-page 2"))}]}]])
