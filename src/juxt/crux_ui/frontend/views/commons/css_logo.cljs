(ns juxt.crux-ui.frontend.views.commons.css-logo
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [clojure.string :as s]
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
      #_{:background :white
         :z-index 10
         :width :50vh
         :height :50vh}

      [:&__cube
       {:width :60px
        :height :60px
        :flex "0 0 60px"}
       #_{:position :fixed
          :background :white
          :top :70px
          :width :80%
          :height :80%}
       [:>.svg-cube
        {:height :100%
         :width :83.3%}]]
      [:&__text
       {:display "flex"
        :justify-content "space-between"
        :letter-spacing "0.15em"
        :font-weight "400"
        :flex "0 0 250px"
        :align-items "center"}]])])


(defn root []
  [:div.css-logo
   css-logo-styles
   [:div.css-logo__cube (cube/cube {:animating? false})]
   [:div.css-logo__text "[?console]"]])
