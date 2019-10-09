(ns crux-ui.views.second-layer
  (:require [crux-ui.views.settings :as settings]
            [crux-ui.views.sidebar :as sidebar]
            [crux-ui.views.overview :as overview]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]))

(def ^:private main-pane-views
  {:db.ui.second-layer.main-pane/overview overview/root
   :db.ui.second-layer.main-pane/settings settings/root})

(def ^:private root-styles
  [:style
   (garden/css
     [:.second-layer
      {:width :100%
       :height :100%
       :display :grid
       :grid-gap :16px
       :grid-template "'side main' 1fr / 264px 1fr"}
      [:&__side
       {:grid-area :side
        :overflow :scroll}]
      [:&__main
       {:grid-area :main
        :background :white
        :border-radius :2px
        :height :100%
        :overflow :scroll}]]
     (gs/at-media {:max-width :1000px}
       [:.second-layer
        {:grid-template "'main' 1fr / 1fr"}
        [:&__side
         {:grid-area :main}]
        [:&__main
         {:grid-area :main}]]))])

(defn autoclose-on-fader-click [evt]
  (let [target (.-target evt)]
    (if (= "second-layer" (.-id target))
      (rf/dispatch [:evt.ui.second-layer/toggle]))))

(defn root []
  (let [-sub-second-layer-main-pane (rf/subscribe [:subs.db.ui.second-layer/main-pane])]
    (fn []
      [:div#second-layer.second-layer {:on-click autoclose-on-fader-click}
       root-styles
       [:div.second-layer__side [sidebar/root]]
       (let [mpv (main-pane-views @-sub-second-layer-main-pane)]
         (if mpv
           [:div.second-layer__main [mpv]]))])))

