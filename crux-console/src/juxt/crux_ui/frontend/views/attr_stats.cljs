(ns juxt.crux-ui.frontend.views.attr-stats
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query.results-table :as q-results-table]
            [juxt.crux-ui.frontend.views.style :as s]))

(def ^:private -sub-attr-stats (rf/subscribe [:subs.query/attr-stats-table]))

(def style
  (garden/css
    [:.q-table
     {:border          s/q-ui-border
      :border-top      :none
      :border-left     :none
      :border-collapse :separate
      :border-radius   :2px
      :width           :100%
      :position        :relative}

     ["&__body-cell"
      "&__head-cell"
       {:border-left s/q-ui-border
        :border-top  s/q-ui-border
        :padding     "6px 12px"}]
     ["&__head-cell"
      {:border-top :none
       :background :white
       :font-weight 400
       :letter-spacing :.09em}
      [:&:first-child
       {:border-left :none}]]
     ["&__body-cell"
      {:letter-spacing :.04em}
      [:&:first-child
       {:border-left :none}]]]))

(defn root []
  [:div.attr-stats
   [q-results-table/root @-sub-attr-stats]])
