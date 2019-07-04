(ns juxt.crux-ui.frontend.views.attr-stats
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query.results-table :as q-results-table]
            [juxt.crux-ui.frontend.views.style :as s]))

(def ^:private -sub-attr-stats (rf/subscribe [:subs.query/attr-stats-table]))

(def style
  [:style
    (garden/css
      [:.attr-stats
       [:&__header
        {:padding :16px
         :border-bottom s/q-ui-border}]])])



(defn root []
  [:div.attr-stats
   style
   [:header.attr-stats__header [:strong "Indexed attributes frequencies"]]
   [q-results-table/root @-sub-attr-stats]])
