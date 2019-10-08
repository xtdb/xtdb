(ns crux-ui.views.overview
  (:require [crux-ui.views.settings :as settings]
            [crux-ui.views.sidebar :as sidebar]
            [crux-ui.views.commons.tiny-components :as comps]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [crux-ui.config :as cfg]))

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
        :box-shadow "0 0px 0px 1px hsl(0, 0%, 75%)"}]
      [:&__section
       {:margin-top :32px}]
      [:&__footer
       {:margin-top :48px}]]
     (gs/at-media {:max-width :1000px}
       [:.overview
        {}
        [:&__section--shortcuts
         {:display :none}]]))])

(defn close []
  (rf/dispatch [:evt.ui.second-layer.main-pane/cancel]))

(defn root []
  [:div.overview
   root-styles
   [:h1.overview__title "Console Overview"]
   [:section.overview__section.overview__section--shortcuts
    [:h2.overview__header "Features"]
    [:section.overview__section
     [:h3.overview__header "Attribute history"]]
    [:section.overview__section
     [:h3.overview__header "Examples import"]
     [:a {:href cfg/url-examples-gist :target :_blank} "Import example"]]]
   [:section.overview__section
    [:h2.overview__header "Limitations"]
    [:div.g-mt-2 "When you execute a normal query (not tx) not longer than 2000
     characters it will be placed into your address bar so you can share it."]]
   [:section.overview__section.overview__section--shortcuts
    [:h2.overview__header "Shortcuts"]
    [:div.g-mt-2 [:label "Query submit"] [:kbd "ctrl + enter"]]
    [:div.g-mt-2 [:label "Toggle editor"] [:kbd "ctrl + e"]]
    [:div.g-mt-2 [:label "Toggle fullscreen"] [:kbd "ctrl + cmd + f"]]]
   [:footer.overview__footer
    [comps/button-textual
     {:text "Cancel"
      :on-click close}]]])
