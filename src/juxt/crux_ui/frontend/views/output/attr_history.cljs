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

(defn attr-layout [attr]
  {:title (str (pr-str attr) " over time")
   :xaxis {:title "Valid Time"}
   :yaxis {:title (pr-str attr)}})

(defn root []
  [:div.attr-history
   (let [{:keys [traces attribute] :as p-data} @-sub-plotly-data]
     [charts/plotly-wrapper traces (attr-layout attribute)])])

