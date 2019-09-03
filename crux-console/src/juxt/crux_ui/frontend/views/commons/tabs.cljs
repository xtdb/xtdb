(ns juxt.crux-ui.frontend.views.commons.tabs
  (:require [garden.core :as garden]
            [reagent.core :as r]))

(def tabs-styles
  [:style
   (garden/css
     [:.tabs
      {:display "flex"
       :justify-content "space-between"
       :letter-spacing ".08em"
       :text-transform :uppercase
       :font-size :13px
       :align-items "center"}
      [:&__item
       {:cursor :pointer}
       [:&--active
        {:font-weight 600}]]
      [:&__sep
       {:padding "0 8px"}]
      [:&--header
       {:font-size "20px"}
       [:>.tabs__sep
        {:padding "16px"}]]
      ])])

(defn root [{:tabs/keys [tabs on-tab-activate active-id]}]
  [:div.tabs
   tabs-styles
   (butlast
    (interleave
      (for [{:tabs/keys [id title href] :as tab} tabs]
        ^{:key id}
        [:a.tabs__item.g-nolink
         (cond->
           {:class (if (= id active-id) "tabs__item--active")}
           href (assoc :href href)
           (not href) (assoc :on-click (r/partial on-tab-activate id)))
         title])
      (map (fn [i] ^{:key i} [:div.tabs__sep "/"]) (range))))])
