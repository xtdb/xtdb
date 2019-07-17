(ns juxt.crux-ui.frontend.views.charts.line-chart
  (:require ["plotly.js-dist" :as Plotly]
            [reagent.core :as r]
            [garden.core :as css]
            [garden.core :as garden]
            [cljs.tools.reader.edn :as edn]
            [juxt.crux-ui.frontend.logging :as log]))


(def ^:private plot-styling
  [:style
    (garden/css
      [:.plotly-container
       {:height :100%}])])

(def colors
  #{"YIGnBu" "Portland" "Picnic"})

(defn z-data [{:keys [plain with-cache] :as query-data}]
  (clj->js
    [{:z (take 11 (map #(take 10 %) (:data plain)))
      :name "Cache off"
      :colorscale "YIOrRd"
      :type "surface"}
     {:z  (:data with-cache)
      :name "Cache on"
      :colorscale "Viridis"
      :type "surface"}]))


(defn axis [{:keys [title ticks] :as axis}]
  {:title  (name (:title axis))
   :nticks (count ticks)})

(defn opts [{:keys [title with-cache plain] :as query-data}]
  (clj->js
    {:title title
     :autosize true
     :showlegend true
     :height 900
     :width  1200
     :scene
     {:xaxis (axis (:x with-cache)) ; x is history days
      :yaxis (axis (:y with-cache)) ; y is stocks count
      :zaxis {:title "ms"}}
     :margin
     {:l 65,
      :r 50,
      :b 65,
      :t 90}}))

(defn do-plot [container query-data]
  (.newPlot Plotly container (z-data query-data) (opts query-data)))

(defn root []
  [:div.placeholder])

(defn root
  [{:keys [headers rows] :as table}]
  (let [-inst      (atom nil)]
    (r/create-class
     {:component-did-mount
      (fn [this]
        (let [el   (r/dom-node this)
              data q3-data
              inst (do-plot "plotly-container" data)]
          (reset! -inst inst)))

      :reagent-render
      (fn [_ _ _]
        [:div#plotly-container.plotly-container
         [:style plot-styling]])})))
