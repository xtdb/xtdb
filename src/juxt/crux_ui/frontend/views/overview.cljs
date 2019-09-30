(ns juxt.crux-ui.frontend.views.overview
  (:require [juxt.crux-ui.frontend.views.settings :as settings]
            [juxt.crux-ui.frontend.views.sidebar :as sidebar]
            [garden.core :as garden]
            [re-frame.core :as rf]))

(def ^:private root-styles
  [:style
   (garden/css
     [:.overview
      {:padding "16px 32px"
       :width :100%}
      [:label
       {:display :inline-block
        :width :200px}]
      [:.g-mt-2
       {:margin-top :16px}]
      [:kbd
       {:padding "4px 6px"
        :background :white
        :border-radius :2px
        :box-shadow "0 0px 0px 1px hsl(0, 0%, 75%)"}]])])


(defn root []
  [:div.overview
   root-styles
   [:h1.overview__title "Console Overview"]
   [:section
    [:h2.overview__header "Shortcuts"]
    [:div.g-mt-2 [:label "Query submit"] [:kbd "ctrl + enter"]]
    [:div.g-mt-2 [:label "Toggle editor"] [:kbd "ctrl + e"]]
    [:div.g-mt-2 [:label "Toggle fullscreen"] [:kbd "ctrl + cmd + f"]]]])
