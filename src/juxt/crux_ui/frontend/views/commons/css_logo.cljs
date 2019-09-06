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
      [:&__cube
       {:width :60px
        :height :60px
        :flex "0 0 60px"}
       [:>.svg-cube
        {:height :100%
         :width :83.3%}]]
      [:&__text
       {:display "flex"
        :justify-content "space-between"
        :letter-spacing "0.15em"
        :font-weight "400"
        :flex "0 0 250px"
        :align-items "center"}]]
     (gs/at-media {:max-width :500px}
      [:.css-logo__text
       {:display :none}])
     (gs/at-media {:max-width :375px}
      [:.css-logo__cube
       {:width :40px
        :height :40px
        :flex "0 0 40px"}]))])


(defn root []
  [:div.css-logo
   css-logo-styles
   [:div.css-logo__cube (cube/cube {:animating? false})]
   [:div.css-logo__text "[?console]"]])
