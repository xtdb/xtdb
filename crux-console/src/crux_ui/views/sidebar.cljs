(ns crux-ui.views.sidebar
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [clojure.string :as s]
            [crux-ui.views.commons.cube-svg :as cube]
            [crux-ui.views.functions :as vf]
            [crux-ui.routes :as routes]
            [crux-ui.views.commons.css-logo :as css-logo]
            [re-frame.core :as rf]
            [crux-ui.views.commons.tiny-components :as comps]
            [crux-ui.config :as cfg]
            [reagent.core :as r]))


(def ^:private sidebar-styles
  [:style
   (garden/css
     [:.sidebar
      {:width :100%
       :min-height :100%
       :background :white
       :border-radius :2px
       :padding "0px 0px 48px"}
      [:&__item
       {:cursor :pointer
        :padding "16px 24px"}
       [:&:hover
        {:background "hsla(0, 0%, 0%, 0.1)"}]
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


(defn -item [{:keys [dispatch] :as attrs} contents]
  [:div.sidebar__item
   (cond-> attrs
     dispatch (-> (assoc :on-click (r/partial rf/dispatch dispatch))
                  (dissoc :dispatch)))
   contents])


(defn root []
  (let [-sub-examples (rf/subscribe [:subs.query/examples])
        -sub-qmap?    (rf/subscribe [:subs.query/is-query-map?])]
    (fn []
      [:div.sidebar
       sidebar-styles
       [-item {:class    "sidebar__item--logo"
               :dispatch [:evt.ui.second-layer/toggle]} [css-logo/root]]
       [-item {:dispatch [:evt.ui/fullscreen]} "Fullscreen mode"]
       (if @-sub-qmap?
         [-item {:dispatch [:evt.ui/toggle-polling]} "Toggle polling"])
       [-item {:dispatch [:evt.ui.sidebar/show-settings]} "Settings"]
       [-item {:dispatch [:evt.ui.sidebar/show-overview]} "Console Overview"]
       (if-not @-sub-examples
         [-item {:dispatch [:evt.ui/restore-examples]} "Restore Examples"])
       [-item {:dispatch [:evt.ui/import-examples]} "Import Examples"]
       [-item {} [comps/link-outer cfg/url-docs "Crux Docs"]]
       [-item {} [comps/link-outer cfg/url-chat "Crux Chat"]]
       [-item {} [comps/link-mailto cfg/url-mail]]])))

(comment
  (routes/path-for :rd/query-ui)
  (routes/path-for :rd/settings))
