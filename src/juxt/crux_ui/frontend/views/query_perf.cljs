(ns juxt.crux-ui.frontend.views.query-perf
  (:require [garden.core :as garden]
            [juxt.crux-ui.frontend.views.charts.query-perf-plots :as perf-plots]
            [cljs.reader :as edn]))


(def plots-data
  (if-let [script (js/document.getElementById "plots-data")]
    (edn/read-string (.-textContent script))
    (println :plots-data-not-found)))

(def q1-data
  {:title "Simple query execution time"
   :plain (:q1 plots-data)
   :with-cache (:q1-with-cache plots-data)})

(def q3-data
  {:title "Query with joins execution time"
   :plain (:q3 plots-data)
   :with-cache (:q3-with-cache plots-data)})


(def ^:private root-styles
  [:style
   (garden/css
     [[:a]])])

(defn root []
  [:div.query-perf
   [:div.query-perf__plot1
     [perf-plots/root q1-data]]
   [:div.query-perf__plot1
     [perf-plots/root q3-data]]])
