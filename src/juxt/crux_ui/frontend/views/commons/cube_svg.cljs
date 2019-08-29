(ns juxt.crux-ui.frontend.views.commons.cube-svg
  (:require [garden.stylesheet :as gs]
            [clojure.string :as s]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.functions :as vu]))


(def color-gray "rgb(201, 201, 201)")
(def color-black "rgb(1, 1, 1)")


(def kf-pulse-gray-orange
  (let [frame1
        {:stroke color-gray
         :fill color-gray}]
    (gs/at-keyframes :pulse-gray-orange
                     [:0% frame1]
                     [:33% {:stroke :orange
                            :fill :orange}]
                     [:66% frame1])))

(def kf-pulse-black-orange
  (let [frame1
        {:stroke color-black
         :fill color-black}]
    (gs/at-keyframes :pulse-black-orange
                     [:0% frame1]
                     [:33% {:stroke :orange
                            :fill :orange}]
                     [:66% frame1]
                     [:100% frame1])))

(def cube-animation
  (let [anim-for-gray "pulse-gray-orange"
        anim-for-black "pulse-black-orange"
        duration-ms 1000

        ms #(str % "ms")

        ae
        (fn [anim-name animation-order]
          (let [delay-ms (* (/ duration-ms 3) animation-order)]
            (s/join " "
                    [anim-name
                     (ms duration-ms)
                     (ms delay-ms)
                     "infinite"])))

        animated
        (list
         [:.rib--grey.rib--bottom-left
          {:animation (ae anim-for-gray 0)}]
         [:.rib--bottom-front
          {:animation (ae anim-for-black 0)}]
         [:.rib--bottom-right
          {:animation (ae anim-for-black 0)}]
         [:.rib--grey.rib--bottom-back
          {:animation (ae anim-for-gray 0)}]


         [:.rib--grey.rib--center-back-left
          {:animation (ae anim-for-gray 1)}]
         [:.rib--black.rib--center-right
          {:animation (ae anim-for-black 1)}]


         [:.rib--top-front
          :.rib--top-back
          :.rib--top-left
          :.rib--orange.rib--top-right
          {:animation (ae anim-for-black 2)}])]
    animated))

(def style
  [:style
   (garden/css
     kf-pulse-black-orange
     kf-pulse-gray-orange
     [:.svg-cube
      {:width :83.3%
       :height :100%}
      [:&--animating
       cube-animation]]
     [:.rib--grey
      {:stroke  "rgb(201, 201, 201)"
       :stroke-width "5px"
       :stroke-miterlimit "10px"}]
     [:.rib6
      {:stroke-width "6px"}]
     [:.rib--black
      {:stroke "#000"}]
     [:.rib--orange
      {:fill "rgb(248, 150, 29)"}])])



(defn cube [{:keys [animating?]}]
  [:svg
   {:class (vu/bem-str :svg-cube {:animating animating?})
    :version "1.1" :x "0px" :y "0px" :viewBox "0 0 500 598.837"
    :xmlns "http://www.w3.org/2000/svg"}
   style
   [:line.rib.rib--grey.rib--bottom-left {:x1 "250.911" :y1 "249.156" :x2 "19.097" :y2 "421.962"}]
   [:line#line14.rib.rib--grey.rib--center-back-left {:x1 "249.528" :y1 "252.244" :x2 "250.555" :y2 "4.264"}]
   [:line.rib.rib--grey.rib--bottom-back {:x1 "250.649" :y1 "250.835" :x2 "483.864" :y2 "419.57"}]
   [:line.rib.rib--black.rib--center-right.rib6 {:x1 "483.578" :y1 "419.286" :x2 "483.578" :y2 "174.755"}]
   [:polygon.rib.rib--top-front {:points "16.193 181.85 247.778 351.14 249.908 352.694 249.956 352.646 249.956 343.685 249.908 343.685 16.193 172.844"}]
   [:polygon.rib.rib--bottom-front {:points "16.199 427.682 246.538 591.662 248.655 593.164 248.704 593.118 248.704 584.438 248.655 584.438 16.199 418.959"}]
   [:polygon.rib.rib--bottom-right {:points "248.299 583.672 484.859 415.128 487.034 413.583 487.084 413.631 487.084 422.553 487.034 422.553 248.299 592.638" :transform "matrix(-1, 0, 0, -1, 735.383011, 1006.221008)"}]
   [:polygon.rib.rib--top-back {:points "251.976 11.002 485.873 178.546 488.041 180.087 488.091 180.039 488.091 171.148 488.041 171.148 252.95 3.762" :transform "matrix(-1, 0, 0, -1, 738.143997, 181.72001)"}]
   [:polygon.rib.rib--top-left {:points "16.268 171.283 247.892 2.993 250.021 1.45 250.07 1.498 250.07 10.405 250.021 10.405 16.268 180.236"}]
   [:polygon.rib.rib--orange.rib--top-right {:points "249.956 343.562 250.005 343.562 250.005 352.646 486.649 180.05 486.535 170.226"}]])
