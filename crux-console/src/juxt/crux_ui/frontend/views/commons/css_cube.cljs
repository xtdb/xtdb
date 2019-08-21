(ns juxt.crux-ui.frontend.views.commons.css-cube
  (:require [juxt.crux-ui.frontend.views.functions :as vf]
            [garden.core :as garden]))

(def ^:private cube-styles
  [:style
   (garden/css
     [:.cube
      {:display :block
       ;:outline "1px solid blue"
       :height :50px
       :position :relative
       :width :50px
       :transform-origin "50% 50% -25px"
       :transform-style "preserve-3d"
       :transform "rotateX(-45deg) rotateY(55deg)"}
      (let [rib-length "100%"
            rib-thickness  "1px"
            transform-back "translateZ(-50px)"
            transform-left "rotateY(90deg)"
            transform-right "rotateY(-90deg)"]
        [:&__rib
         {:width rib-length
          :height rib-thickness
          :background :black
          :position :absolute}
         [:&--center
          {:height rib-length
           :width rib-thickness
           :bottom 0}]
         [:&--bottom
          {:bottom 0}]
         [:&--left
          {:right 0
           :transform-origin "0%"
           :transform transform-left}]
         [:&--right
          {:right 0
           :transform-origin "100%"
           :transform transform-right}]
         [:&--top&--right
          {:background :orange}]
         [:&--center&--right
          :&--center&--left
          {:transform :none}]
         [:&--back
          :&--back&--center
          {:transform transform-back}]
         [:&--center&--back
          {}]])])])

(defn rib [& modifiers]
  [:div (apply vf/bem (cons :cube__rib modifiers))])

(defn crux-cube []
  [:div.cube
   cube-styles
   [rib :top :front]
   [rib :top :left]
   [rib :top :back]
   [rib :top :right]
   ;
   [rib :center :left]
   [rib :center :right]
   [rib :center :back :left]
   [rib :center :back :right]
   ;
   [rib :bottom :front]
   [rib :bottom :left]
   [rib :bottom :back]
   [rib :bottom :right]])
