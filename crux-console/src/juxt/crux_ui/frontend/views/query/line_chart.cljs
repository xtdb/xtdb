(ns juxt.crux-ui.frontend.views.query.line-chart
  (:require ["plotly.js-gl3d-dist" :as Plotly]
            [reagent.core :as r]
            [garden.core :as css]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.logging :as log]))


(def ^:private plot-styling
  [:style
    (garden/css
      [:.plot
       {:background :blue
        :height :100%}])])

(defn my-rand []
  (.toFixed (* 100 (rand)) 4))

(defn gen-v [f c]
  (vec (take c (repeatedly f))))

(def z-data
  (gen-v #(gen-v my-rand 20) 20))

(def data
  (clj->js
    [{:z z-data
      :type "surface"}]))

(def opts
  (clj->js
    {:title "Mt Bruno Elevation",
     :autosize false,
     :width 500,
     :height 500,
     :xaxis {:range [0 21]}
     :yaxis {:range [0 21]}
     :zaxis {:range [0 100]}
     :margin
     {:l 65,
      :r 50,
      :b 65,
      :t 90}}))



(log/log data)
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
