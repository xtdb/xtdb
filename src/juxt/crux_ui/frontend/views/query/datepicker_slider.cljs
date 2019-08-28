(ns juxt.crux-ui.frontend.views.query.datepicker-slider
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logging :as log]
            [juxt.crux-ui.frontend.logic.time :as time]
            [juxt.crux-ui.frontend.views.commons.datepicker-native :as dpn]
            ["react-input-range" :as ir]
            [reagent.core :as r]
            [garden.core :as garden]))


(def style
  [:style
   (garden/css
     [:.slider-date-time-picker
      {:width :100%}
      [:&__header
       {:display :flex
        :align-items :center
        :justify-content :space-between}
       [:&-label
        {:font-size :18px
         :font-weight 400
         :letter-spacing :0.09em}]
       [:&-native
        {:width :250px
         :flex "0 0 100px"}]]
      [:&__row
       {:display :flex
        :align-items :center
        :height :28px}
       [:&-label
        {:flex "0 0 60px"
         :text-transform :lowercase
         :font-size :13px}]
       [:&-slider
        {:flex "1 1 auto"}]]])])


(defn root-simple
  [{:keys [value ; components
           ^js/String label
           on-change
           on-change-complete]
    :as prms}]
  (let [on-change-internal
        (fn [time-component nv & [complete?]]
          (let [new-val (assoc value time-component nv)]
            (when (and complete? on-change-complete)
              (println :on-change (time/comps->date new-val))
              (on-change-complete (time/comps->date new-val)))
            (when on-change
              (println :on-change (time/comps->date new-val))
              (on-change (time/comps->date new-val)))))]
    [:div.slider-date-time-picker
     [:div.slider-date-time-picker__header
      [:label.slider-date-time-picker__header-label label]
      [:div.slider-date-time-picker__header-native
       [dpn/picker
        {:value     (time/comps->date value)
         :on-change on-change}]]]
     [:div.slider-date-time-picker__row
      [:div.slider-date-time-picker__row-label "Year"]
      [:div.slider-date-time-picker__row-slider
       [:> ir {:value            (get value :time/year)
               :step             1
               :classNames      #js  {:activeTrack "input-range__track input-range__track--active",
                                      :disabledInputRange "input-range input-range--disabled",
                                      :inputRange "input-range",
                                      :labelContainer "input-range__label-container",
                                      :maxLabel "input-range__label input-range__label--max",
                                      :minLabel "input-range__label input-range__label--min",
                                      :slider "input-range__slider",
                                      :sliderContainer "input-range__slider-container",
                                      :track "input-range__track input-range__track--background",
                                      :valueLabel "input-range__label input-range__label--value-year",}

               :minValue         1970
               :maxValue         2020
               :onChangeComplete #(on-change-internal :time/year % true)
               :onChange         #(on-change-internal :time/year %)}]]]
     [:div.slider-date-time-picker__row
      [:div.slider-date-time-picker__row-label "Month"]
      [:div.slider-date-time-picker__row-slider
       [:> ir {:value            (get value :time/month)
               :step             1
               :minValue         1
               :maxValue         12
               :onChangeComplete #(on-change-internal :time/month % true)
               :onChange         #(on-change-internal :time/month %)}]]]
     [:div.slider-date-time-picker__row
      [:div.slider-date-time-picker__row-label "Day"]
      [:div.slider-date-time-picker__row-slider
       [:> ir {:value            (get value :time/date)
               :step             1
               :minValue         1
               :maxValue         31
               :onChangeComplete #(on-change-internal :time/date % true)
               :onChange         #(on-change-internal :time/date %)}]]]
     [:div.slider-date-time-picker__row
      [:div.slider-date-time-picker__row-label "Hour"]
      [:div.slider-date-time-picker__row-slider
       [:> ir {:value            (get value :time/hour)
               :step             1
               :minValue         0
               :maxValue         23
               :onChangeComplete #(on-change-internal :time/hour % true)
               :onChange         #(on-change-internal :time/hour %)}]]]
     [:div.slider-date-time-picker__row
      [:div.slider-date-time-picker__row-label "Minute"]
      [:div.slider-date-time-picker__row-slider
       [:> ir {:value            (get value :time/minute)
               :step             1
               :minValue         0
               :maxValue         59
               :onChangeComplete #(on-change-internal :time/minute % true)
               :onChange         #(on-change-internal :time/minute %)}]]]]))
