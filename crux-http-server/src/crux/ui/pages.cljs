(ns crux.ui.pages
  (:require
   [crux.ui.events :as events]))

(def pages
  {:query {:dispatch [::events/fetch-query-table]}
   :entity {:dispatch [::events/fetch-entity]}})
