(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as subs]
   [reagent.core :as r]
   [crux.uikit.table :as table]
   [re-frame.core :as rf]))

(defn view []
  (let [metadata @(rf/subscribe [::subs/metadata])]
    [:div.container
     [table/table metadata]
     #_[:pre
      (with-out-str
        (pprint/pprint metadata))]]))
