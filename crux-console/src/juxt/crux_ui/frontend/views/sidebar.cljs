(ns juxt.crux-ui.frontend.views.sidebar
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [clojure.string :as s]
            [juxt.crux-ui.frontend.views.commons.cube-svg :as cube]
            [juxt.crux-ui.frontend.views.functions :as vf]
            [juxt.crux-ui.frontend.routes :as routes]
            [juxt.crux-ui.frontend.views.commons.css-logo :as css-logo]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.tiny-components :as comps]
            [juxt.crux-ui.frontend.config :as cfg]))


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


(defn -item [attrs contents]
  [:div.sidebar__item attrs contents])

(defn root []
  [:div.sidebar
   css-logo-styles
   [:div.sidebar__item.sidebar__item--logo
    {:on-click dispatch-sidebar-toggle}
    [css-logo/root]]
   [-item {:on-click (rf/dispatch [:evt.ui/fullscreen])}
    "Fullscreen"]
   [-item {} "Polling"]
   [-item {} "Settings"]
   [-item {} "Shortcut"]
   [-item {} "Console Overview"]
   [-item {} "Restore Examples"]
   [-item {} [comps/link-outer cfg/url-docs "Crux Docs"]]
   [-item {} [comps/link-outer cfg/url-chat "Crux Chat"]]
   [-item {} [comps/link-mailto cfg/url-mail]]])

(comment
  (routes/path-for :rd/query-ui)
  (routes/path-for :rd/settings))
