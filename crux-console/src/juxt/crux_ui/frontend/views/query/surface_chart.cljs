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

(def data
  (clj->js
    [{:z (take 9 (map #(take 9 %) (:data q1-data)))
      :colorscale "Viridis"
      :type "surface"}
     {:z (take 9 (map #(take 9 %) (:data q1-cache-data)))
      :type "surface"} ]))


(defn axis [{:keys [title ticks] :as axis}]
  {:title  (name (:title axis))
   :tick0  (first ticks)
   :showticklabels true
   :dtick  (- (nth ticks 2) (second ticks))
   :nticks (count ticks)})

(def opts
  (clj->js
    {:title "Query-1 avg execution time"
     :autosize false
     :showlegend true
     :height 800
     :width  1200
     :scene
     {:xaxis (axis (:x q1-data))
      :yaxis (axis (:y q1-data))
      :zaxis {:title "time"}}
     :margin
     {:l 65,
      :r 50,
      :b 65,
      :t 90}}))



(log/log data)
(set! js/window.data2 (clj->js q1-data))
(log/log js/window.data2)
(set! js/window.data data)
(set! js/window.layout opts)
(set! js/window.Plotly Plotly)


(defn root
  [{:keys [headers rows] :as table}]
  (let [-inst      (atom nil)]
    (r/create-class
     {:component-did-mount
      (fn [this]
        (let [el   (r/dom-node this)
              inst (.newPlot Plotly "plotly-container" data opts)]
          (reset! -inst inst)))

      :reagent-render
      (fn [_ _ _]
        [:div#plotly-container.plotly-container
         [:style plot-styling]])})))
