(ns crux-ui.views.commons.datepicker-native
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [crux-ui.logging :as log]
            [crux-ui.functions :as f]
            [crux-ui.logic.time :as time]
            [crux-ui.config :as cfg]
            [crux-ui.views.commons.input :as input]
            [crux-ui.views.commons.keycodes :as kc]
            [reagent.core :as r]
            [crux-ui.views.commons.dom :as dom]
            [crux-ui.views.functions :as vu]))


(defn- on-time-change--native [on-change-external evt]
  (try
    (let [v (f/jsget evt "target" "value")
          ts (js/Date.parse v)]
      (if (js/isNaN ts)
        (on-change-external nil)
        (on-change-external (js/Date. ts))))
    (catch js/Error err
      (on-change-external nil)
      (log/error err))))


(def style
  [:style
   (garden/css
     [:.native-date-time-picker
      {:display :flex
       :font-size :14px
       :align-items :center}
      [:&__label
       {:width :136px
        :display :block
        :letter-spacing :.04em}]
      [:&__input
       input/styles-src
       {:padding "4px 0"
        :width :auto}]]
     (gs/at-media {:min-width :1000px}
       [:.native-date-time-picker--column
        {:flex-direction :column
         :align-items :flex-start}
        ["> .native-date-time-picker__label"
         {:font-size :16px
          :width :auto
          :letter-spacing :0.09em
          :color :gray}]
        ["> .native-date-time-picker__input"
         {:line-height :32px}]]))])



(defn picker
  [{:keys
    [label
     ui/layout
     ^js/Date value
     on-change]
    :as prms}]
  (let [state (r/atom {:value value})
        on-commit-internal  (r/partial on-time-change--native on-change)]
    (fn []
      [:div (vu/bem :native-date-time-picker layout)
       (if label
         [:label.native-date-time-picker__label label])
       [:input.native-date-time-picker__input
        (let [v (:value @state)]
          {:type         "datetime-local"
           :placeholder  (if-not cfg/supports-input-datetime? "dd/mm/yyyy, --:--")
           :defaultValue (if v (time/format-for-dt-local v))
           :on-key-down
           (dom/dispatch-on-keycode
             {::kc/enter on-commit-internal})
           :on-blur      on-commit-internal})]])))
