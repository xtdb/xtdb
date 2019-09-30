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
       :width :100%}])])

(defn root []
  [:div.overview
   root-styles
   [:h1.overview__title "Console Overview"]
   [:section
    [:h2.overview__header "Shortcuts"]
    [:h3.overview__header "Query submit"]
    [:h3.overview__header "Toggle editor"]
    [:h3.overview__header "Fullscreen"]]])
