(ns juxt.crux-ui.frontend.views.output.attr-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.charts.wrapper-basic :as charts]))

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
     (if p-data
       [charts/plotly-wrapper traces (attr-layout attribute)]
       "No data to display, try to run a query that will include a numeric attribute"))])

