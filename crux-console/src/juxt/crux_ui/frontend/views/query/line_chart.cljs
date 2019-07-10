(ns juxt.crux-ui.frontend.views.query.line-chart
  (:require ["plotly.js/lib/core" :as Plotly]
            [reagent.core :as r]
            [garden.core :as css]
            [garden.core :as garden]))

(def csv-src "https://raw.githubusercontent.com/plotly/datasets/master/api_docs/mt_bruno_elevation.csv")

(def d3 (.-d3 Plotly))

(.csv d3 csv-src #(println :loaded))

(.csv (.-d3 Plotly))


(def ^:private plot-styling
  [:style
    (garden/css
      [:.plot
       {:background :blue
        :height :100%}])])

(def z-data
  [[1 2 1]
   [2 3 2]
   [1 2 1]])

(def data
  #js [#js {:z z-data
            :type "surface"}])


(defn root
  [{:keys [headers rows] :as table}]
  (let [-inst      (atom nil)]
    (r/create-class
     {:component-did-mount
      (fn [this]
        (let [el   (r/dom-node this)
              opts
              #js {:title "Mt Bruno Elevation",
                   :autosize false,
                   :width 500,
                   :height 500,
                   :margin
                   #js {:l 65,
                        :r 50,
                        :b 65,
                        :t 90}}
              inst (.newPlot Plotly el data opts)]
          (reset! -inst inst)))

      :reagent-render
      (fn [_ _ _]
        [:div#plotly-container.plotly-container
         [:style plot-styling]])})))
