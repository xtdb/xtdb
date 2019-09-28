(ns juxt.crux-ui.frontend.views.second-layer
  (:require [juxt.crux-ui.frontend.views.settings :as settings]
            [juxt.crux-ui.frontend.views.sidebar :as sidebar]
            [garden.core :as garden]
            [re-frame.core :as rf]))

(defn overview []
  [:div.overview
   "Shortcuts"
   "Query submit"
   "Toggle editor"
   "Fullscreen"])

(def ^:private main-pane-views
  {:db.ui.second-layer.main-pane/overview overview
   :db.ui.second-layer.main-pane/settings settings/root})

(def ^:private root-styles
  [:style
   (garden/css
     [:.second-layer
      {:width :100%
       :height :100%}])])


(defn root []
  (let [-sub-second-layer-main-pane (rf/subscribe [:subs.db.ui.second-layer/main-pane])]
    (fn []
      (let [mpv (main-pane-views @-sub-second-layer-main-pane)]
        [:div.second-layer
         root-styles
         [:div.second-layer__side
          [sidebar/root]]
         [:div.second-layer__main
          (if mpv [mpv])]]))))

