(ns juxt.crux-ui.frontend.views.commons.css-logo
  (:require [garden.core :as garden]
            [juxt.crux-ui.frontend.views.commons.cube-svg :as cube]
            [juxt.crux-ui.frontend.views.functions :as vf]))

(def ^:private css-logo-styles
  [:style
   (garden/css
     [:.css-logo
      {:display :flex
       :justify-content :space-between
       :align-items :center
       :width "100%"}
      [:&__cube]
      [:&__text
       {:display "flex"
        :justify-content "space-between"
        :letter-spacing "0.15em"
        :margin-left :1.0em
        :font-weight "400"
        :flex "0 0 250px"
        :align-items "center"}]])])

(defn root []
  [:div.css-logo
   css-logo-styles
   [:div.css-logo__cube cube/cube]
   [:div.css-logo__text "[?console]"]])
