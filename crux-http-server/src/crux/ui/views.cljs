(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as subs]
   [reagent.core :as r]
   [re-frame.core :as rf]))

(defn view []
  (rf/dispatch [::events/get-query-result])
  (fn []
    (let [query-data @(rf/subscribe [::subs/query-data])]
      [:div
       [:pre
        (with-out-str
          (pprint/pprint query-data))]])))
