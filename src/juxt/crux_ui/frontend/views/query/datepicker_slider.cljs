(ns juxt.crux-ui.frontend.views.query.datepicker-slider
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logging :as log]
            [juxt.crux-ui.frontend.logic.time :as time]
            [juxt.crux-ui.frontend.views.query.datepicker-native :as dpn]
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
        :height :40px}
       [:&-label
        {:flex "0 0 60px"
         :text-transform :lowercase
         :font-size :13px}]
       [:&-slider
        {:flex "1 1 auto"}]]])])


(defn root
  [{:keys [^js/Date value
           value-sub
           ^js/String label
           on-change
           on-change-complete]
    :as prms}]
  (let [vatom (atom (time/date->comps @value-sub))
        ;
        on-change-internal
        (fn [time-component value & [complete?]]
          (swap! vatom assoc time-component value)
          (let [new-val @vatom]
            (if (and complete? on-change-complete)
              (on-change-complete (time/comps->date new-val)))
            (when on-change
              (println :on-change (time/comps->date new-val))
              (on-change (time/comps->date new-val)))))]

    (r/create-class
      {:should-component-update
       (constantly true)
       #_(fn [this cur-argv [f id next-props :as next-argv]]
           (let [active-element js/document.activeElement
                 not-active? (not= (r/dom-node this) active-element)
                 value (time/date->comps @(:value-sub next-props))]
             (and not-active? (not= @vatom value))))

       :component-did-update
       (fn [this old-argv]
         (let [value-sub (:value-sub (r/props this))]
           (reset! vatom (time/date->comps @value-sub))))

       :reagent-render
       (fn -actual-render [{:keys [^js/Date value
                                   value-sub
                                   ^js/String label
                                   on-change
                                   on-change-complete]
                            :as   prms}]
         (let [v (time/date->comps @value-sub)]
           [:div.slider-date-time-picker
            [:div.slider-date-time-picker__header
             [:label.slider-date-time-picker__header-label label]
             [:div.slider-date-time-picker__header-native
              [dpn/picker
               {:value     (time/comps->date v)
                :on-change on-change}]]]
            [:div.slider-date-time-picker__row
             [:div.slider-date-time-picker__row-label "Year"]
             [:div.slider-date-time-picker__row-slider
              [:> ir {:value            (get v :time/year)
                      :step             1
                      :minValue         1970
                      :maxValue         2020
                      :onChangeComplete #(on-change-internal :time/year % true)
                      :onChange         #(on-change-internal :time/year %)}]]]
            [:div.slider-date-time-picker__row
             [:div.slider-date-time-picker__row-label "Month"]
             [:div.slider-date-time-picker__row-slider
              [:> ir {:value            (get v :time/month)
                      :step             1
                      :minValue         1
                      :maxValue         12
                      :onChangeComplete #(on-change-internal :time/month % true)
                      :onChange         #(on-change-internal :time/month %)}]]]
            [:div.slider-date-time-picker__row
             [:div.slider-date-time-picker__row-label "Day"]
             [:div.slider-date-time-picker__row-slider
              [:> ir {:value            (get v :time/date)
                      :step             1
                      :minValue         0
                      :maxValue         31
                      :onChangeComplete #(on-change-internal :time/date % true)
                      :onChange         #(on-change-internal :time/date %)}]]]
            [:div.slider-date-time-picker__row
             [:div.slider-date-time-picker__row-label "Hour"]
             [:div.slider-date-time-picker__row-slider
              [:> ir {:value            (get v :time/hour)
                      :step             1
                      :minValue         0
                      :maxValue         23
                      :onChangeComplete #(on-change-internal :time/hour % true)
                      :onChange         #(on-change-internal :time/hour %)}]]]
            [:div.slider-date-time-picker__row
             [:div.slider-date-time-picker__row-label "Minute"]
             [:div.slider-date-time-picker__row-slider
              [:> ir {:value            (get v :time/minute)
                      :step             1
                      :minValue         0
                      :maxValue         59
                      :onChangeComplete #(on-change-internal :time/minute % true)
                      :onChange         #(on-change-internal :time/minute %)}]]]]))})))

