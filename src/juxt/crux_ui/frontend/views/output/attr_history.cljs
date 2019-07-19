(ns juxt.crux-ui.frontend.views.output.attr-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.charts.core :as charts]))

(def ^:private -sub-plotly-data (rf/subscribe [:subs.query/attr-history-plot-data]))

(def ^:private root-styles
  [:style
   (garden/css
     [:.attr-history
      {}])])

(defn root []
  [:div.attr-history
   [charts/plotly-wrapper @-sub-plotly-data]])

