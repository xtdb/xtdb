(ns juxt.crux-ui.frontend.views.commons.tabs
  (:require [garden.core :as garden]))

(def tabs-styles
  [:style
   (garden/css
     [:.tabs
      {:display "flex"
       :justify-content "space-between"
       :font-size "20px"
       :align-items "center"}
      [:&__sep
       {:padding "16px"}]])])

(defn root [{:keys [tabs on-tab-activate active-tab-id]}]
  [:div.tabs
   tabs-styles
   (interpose
     [:div.tabs__sep "/"]
     (for [{:keys [id title] :as tab} tabs]
       ^{:key id}
       [:div.tabs__item
        (if (= id active-tab-id)
          {:class "tabs__item--active"}
          {:on-click #(on-tab-activate id)})
        title]))])
