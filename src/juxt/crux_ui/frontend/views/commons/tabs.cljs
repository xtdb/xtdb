(ns juxt.crux-ui.frontend.views.commons.tabs
  (:require [garden.core :as garden]))

(def tabs-styles
  [:style
   (garden/css
     [:.tabs
      {:display "flex"
       :justify-content "space-between"
       :font-size "20px"
       :letter-spacing ".03em"
       :align-items "center"}
      [:&__item
       {}
       [:&--active
        {:font-weight 400}]]
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
         (cond->
           {:class (if (= id active-tab-id) "tabs__item--active")}
           href (assoc :href href)
           (not href) (assoc :on-click on-tab-activate))
         title])
      (map (fn [i] ^{:key i} [:div.tabs__sep "/"]) (range))))])
