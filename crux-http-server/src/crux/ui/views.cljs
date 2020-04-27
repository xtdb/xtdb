(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as subs]
   [reagent.core :as r]
   [re-frame.core :as rf]))

(defn view []
  (let [metadata @(rf/subscribe [::subs/metadata])]
    [:div
     [:pre
      (with-out-str
        (pprint/pprint metadata))]]))
