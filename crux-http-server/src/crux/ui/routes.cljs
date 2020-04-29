(ns crux.ui.routes)

(def routes
  [""
   [["/" :homepage]
    ["/_query" :query]
    [["/_entity/" [#".+" :entity-id]] :entity]]])
