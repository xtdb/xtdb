(ns juxt.crux-ui.frontend.views.attr-stats
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.output.table :as q-results-table]
            [juxt.crux-ui.frontend.views.style :as s]))

(def ^:private -sub-attr-stats (rf/subscribe [:subs.query/attr-stats-table]))

(def ^:private style
  [:style
    (garden/css
      [:.attr-stats
       [:&__footer
        {:text-align :center
         :padding :16px}]])])

(defn root []
  [:div.attr-stats
   style
   [q-results-table/root @-sub-attr-stats]
   [:footer.attr-stats__footer [:strong "Indexed attributes frequencies"]]])
