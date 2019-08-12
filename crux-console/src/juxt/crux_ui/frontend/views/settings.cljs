(ns juxt.crux-ui.frontend.views.settings
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query-ui :as q]
            [juxt.crux-ui.frontend.views.header :as header]
            [juxt.crux-ui.frontend.views.comps :as comps]
            [juxt.crux-ui.frontend.svg-icons :as icon]))

(def ^:private -sub-root-tab (rf/subscribe [:subs.ui/root-tab]))

(def ^:private root-styles
  [:style
    (garden/css [])])


(defn root []
  (let [root-tab @-sub-root-tab]
    (fn []
      [:div.settings])))

