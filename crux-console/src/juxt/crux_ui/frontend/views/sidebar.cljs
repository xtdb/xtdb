(ns juxt.crux-ui.frontend.views.sidebar
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [clojure.string :as s]
            [juxt.crux-ui.frontend.views.commons.cube-svg :as cube]
            [juxt.crux-ui.frontend.views.functions :as vf]
            [juxt.crux-ui.frontend.routes :as routes]
            [juxt.crux-ui.frontend.views.commons.css-logo :as css-logo]
            [re-frame.core :as rf]))


(defn dispatch-sidebar-toggle []
  (rf/dispatch [:evt.ui.sidebar/toggle]))

(def ^:private css-logo-styles
  [:style
   (garden/css
     [:.sidebar
      {:width :280px
       :height :100%
       :position :absolute
       :background :white
       :border-radius :2px
       :padding "0px 0px"}
      [:&__item
       {:padding "16px 24px"}
       [:&--logo
        {:padding "12px 16px"}]]]
     (gs/at-media {:max-width :500px}
      [:.css-logo__text
       {:display :none}])
     (gs/at-media {:max-width :375px}
      [:.css-logo__cube
       {:width :40px
        :height :40px
        :flex "0 0 40px"}]))])



(defn root []
  [:div.sidebar
   css-logo-styles
   [:div.sidebar__item.sidebar__item--logo
    {:on-click dispatch-sidebar-toggle}
    [css-logo/root]]
   [:div.sidebar__item "Query UI" (routes/path-for :rd/query-ui)]
   [:div.sidebar__item "Fullscreen"]
   [:div.sidebar__item "Polling"]
   [:div.sidebar__item "Settings" (routes/path-for :rd/settings)]
   [:div.sidebar__item "Shortcuts"]
   [:div.sidebar__item "Console Overview"]
   [:div.sidebar__item "Crux Docs"]
   [:div.sidebar__item "Crux Chat"]
   [:div.sidebar__item "crux@juxt.pro"]])
