(ns juxt.crux-ui.frontend.views.second-layer
  (:require [juxt.crux-ui.frontend.views.settings :as settings]
            [juxt.crux-ui.frontend.views.sidebar :as sidebar]
            [juxt.crux-ui.frontend.views.overview :as overview]
            [garden.core :as garden]
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
        :overflow :scroll}]])])

(defn root []
  (let [-sub-second-layer-main-pane (rf/subscribe [:subs.db.ui.second-layer/main-pane])]
    (fn []
      (let [mpv (main-pane-views @-sub-second-layer-main-pane)]
        [:div.second-layer
         root-styles
         [:div.second-layer__side
          [sidebar/root]]
         (if mpv
           [:div.second-layer__main [mpv]])]))))

