(ns juxt.crux-ui.frontend.views.query.surface-chart
  (:require ["plotly.js-gl3d-dist" :as Plotly]
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

(defn my-rand []
  (.toFixed (* 100 (rand)) 4))

(defn gen-v [f c]
  (vec (take c (repeatedly f))))

(def z-data
  (gen-v #(gen-v my-rand 20) 20))

(def plots-data
  (if-let [script (js/document.getElementById "plots-data")]
    (edn/read-string (.-textContent script))
    (println :plots-data-not-found)))

(def q1-data
  (and plots-data (:q1 plots-data)))

(def q1-cache-data
  (and plots-data (:q1-with-cache plots-data)))

(def q3-data
  (and plots-data (:q3 plots-data)))

(def q3-cache-data
  (and plots-data (:q3-with-cache plots-data)))

(def colors
  #{"YIGnBu" "Portland" "Picnic"})

(def data
  (clj->js
    [{:z (take 11 (map #(take 10 %) (:data q1-data)))
      :name "Cache off"
      :colorscale "YIOrRd"
      :type "surface"}
     {:z  (:data q1-cache-data)
      :name "Cache on"
      :colorscale "Viridis"
      :type "surface"}]))

(def data-q3
  (clj->js
    [{:z (take 11 (map #(take 10 %) (:data q3-data)))
      :name "Cache off"
      :colorscale "YIOrRd"
      :type "surface"}
     {:z  (:data q3-cache-data)
      :name "Cache on"
      :colorscale "Viridis"
      :type "surface"}]))


(defn axis [{:keys [title ticks] :as axis}]
  {:title  (name (:title axis))
   :nticks (count ticks)})

(def opts
  (clj->js
    {:title "Query-1 avg execution time"
     :autosize false
     :showlegend true
     :height 900
     :width  1200
     :scene
     {:xaxis (axis (:x q1-data)) ; x is history days
      :yaxis (axis (:y q1-data)) ; y is stocks count
      :zaxis {:title "ms"}}
     :margin
     {:l 65,
      :r 50,
      :b 65,
      :t 90}}))



(defn root
  [{:keys [headers rows] :as table}]
  (let [-inst      (atom nil)]
    (r/create-class
     {:component-did-mount
      (fn [this]
        (let [el   (r/dom-node this)
              inst (.newPlot Plotly "plotly-container" data-q3 opts)]
          (reset! -inst inst)))

      :reagent-render
      (fn [_ _ _]
        [:div#plotly-container.plotly-container
         [:style plot-styling]])})))
