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
   (butlast
    (interleave
      (for [{:keys [id title href] :as tab} tabs]
        ^{:key id}
        [:a.tabs__item.g-nolink
         {:href href}
         #_(if (= id active-tab-id)
             {:class "tabs__item--active"}
             {:on-click #(on-tab-activate id)})
         title])
      (map (fn [i] ^{:key i} [:div.tabs__sep "/"]) (range))))])
