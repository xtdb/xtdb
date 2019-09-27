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
            [juxt.crux-ui.frontend.config :as cfg]
            [reagent.core :as r]))


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

(defn overview []
  [:div.overview
   "Shortcuts"
   "Query submit"
   "Toggle editor"
   "Fullscreen"])


(defn root []
  (let [-sub-examples (rf/subscribe [:subs.query/examples])
        -sub-qmap?    (rf/subscribe [:subs.query/is-query-map?])]
    (fn []
      [:div.sidebar
       css-logo-styles
       [-item {:class "sidebar__item--logo"
               :dispatch [:evt.ui.sidebar/toggle]} [css-logo/root]]
       [-item {:dispatch [:evt.ui/fullscreen]} "Fullscreen"]
       (if @-sub-qmap?
         [-item {:dispatch [:evt.ui/toggle-polling]} "Toggle polling"])
       [-item {:dispatch [:evt.ui/show-settings]} "Settings"]
       [-item {:dispatch [:evt.ui/show-overview]} "Console Overview"]
       (if-not @-sub-examples
         [-item {:dispatch [:evt.ui/restore-examples]} "Restore Examples"])
       [-item {:dispatch [:evt.ui/import-examples]} "Import Examples"]
       [-item {} [comps/link-outer cfg/url-docs "Crux Docs"]]
       [-item {} [comps/link-outer cfg/url-chat "Crux Chat"]]
       [-item {} [comps/link-mailto cfg/url-mail]]])))

(comment
  (routes/path-for :rd/query-ui)
  (routes/path-for :rd/settings))
