(ns juxt.crux-ui.frontend.views.query-ui
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.query.form :as q-form]
            [juxt.crux-ui.frontend.views.query.output :as q-output]
            [juxt.crux-ui.frontend.views.style :as s]))

(def ^:private query-ui-styles
  [:style
   (garden/css
     [:.query-ui
       {:font-size :16px
        :border-radius :2px
        :margin "0 auto"
        :border s/q-ui-border
        :max-width :1600px
        :width :100%
        :height "calc(100% - 8px)"
        :margin-bottom :8px
       ;:max-height :100%
        :display :grid
        :place-items :stretch

        :grid-template
        "'output' calc(100% - 330px)
         'form' 330px"}

       [:&__output
         {:padding "0px 0"
          :grid-area :output
          :border-bottom s/q-ui-border}]

       [:&__controls
         {:padding "16px 0"
          :border-right s/q-ui-border
          :grid-area :controls}]
         ;:border "1px solid orange"

       [:&__form
         {:padding "0px 0"
          :grid-area :form}]
       [:&--form-minimised
        {:grid-template
         "'output' calc(100% - 50px)
         'form' 50px"}]]

     (gs/at-media
       {:max-width :1000px}
       [:.query-ui
        {:grid-template
         "'output output' calc(100% - 330px)
        'form form' 330px
        / 1fr 1fr"
         :margin-bottom 0}
        [:&--form-minimised
         {:grid-template
          "'output output' calc(100% - 50px)
         'form form' 50px
         / 1fr 1fr"}]
        [:&__controls
         {:display :none}]]))])
         ;:border "1px solid green"


(defn query-ui []
  [:div.query-ui
   query-ui-styles
   [:div.query-ui__output
    [q-output/root]]
   [:div.query-ui__form
    [q-form/root]]])

