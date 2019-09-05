(ns juxt.crux-ui.frontend.views.commons.datepicker-native
  (:require [garden.core :as garden]
            [juxt.crux-ui.frontend.logging :as log]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.logic.time :as time]
            [juxt.crux-ui.frontend.config :as cfg]
            [juxt.crux-ui.frontend.views.commons.input :as input]
            [juxt.crux-ui.frontend.views.commons.keycodes :as kc]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.views.commons.dom :as dom]))


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
        :width :auto}]])])


(defn picker
  [{:keys
    [label
     ^js/Date value
     on-change]
    :as prms}]
  (let [state (r/atom {:value value})
        on-commit-internal  (r/partial on-time-change--native on-change)]
    (fn []
      [:div.native-date-time-picker
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
