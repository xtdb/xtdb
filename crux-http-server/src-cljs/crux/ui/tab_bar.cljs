(ns crux.ui.tab-bar
  (:require [re-frame.core :as rf]))

(defn tab-bar [{:keys [tabs current-tab on-tab-selected] :as opts}]
  (let [selected-tab @(rf/subscribe current-tab)
        [set-selected-tab & selected-tab-args] on-tab-selected]
    [:div.tab-bar
     (for [{:keys [k label]} tabs]
       [:div.tab-bar__tab
        {:key k
         :class (when (= k selected-tab)
                  "tab-bar__tab--selected")
         :on-click #(rf/dispatch (into [set-selected-tab k] selected-tab-args))}
        label])]))
