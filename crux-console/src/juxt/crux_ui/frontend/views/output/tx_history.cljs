(ns juxt.crux-ui.frontend.views.output.tx-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.charts.wrapper-basic :as charts]))

(def ^:private -sub-tx-history (rf/subscribe [:subs.output/tx-history-plot-data]))

(def ^:private root-styles
  [:style
   (garden/css
     [:.tx-history
      {:height :100%}])])

(def tx-layout
  {:title "Queried entities transactions"
   :xaxis {:title "Valid Time"}
   :yaxis {:title "Transaction time"}})

(defn root []
  [:div.tx-history
   root-styles
   (if-let [tx-history @-sub-tx-history]
     [charts/plotly-wrapper tx-history tx-layout]
     [:div.q-output-empty "No data to display, try to run a query"])])
