(ns crux.ui.routes)

(def routes
  ["/ui"
   [["" :homepage]
    ["/_query" :query]
    [["/_entity/" [#".+" :entity-id]] :entity]]])
