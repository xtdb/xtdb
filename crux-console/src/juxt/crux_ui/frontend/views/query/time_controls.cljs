(ns juxt.crux-ui.frontend.views.query.time-controls
  (:require [garden.core :as garden]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.logging :as log]
            [re-frame.core :as rf]))


(defn- on-time-change [time-type evt]
  (try
    (let [v (f/jsget evt "target" "value")
          d (js/Date. v)]
      (log/log "value parsed" d)
      (if (js/isNaN d)
        (rf/dispatch [:evt.ui.query/time-reset time-type])
        (rf/dispatch [:evt.ui.query/time-change time-type d])))
    (catch js/Error err
      (rf/dispatch [:evt.ui.query/time-reset time-type])
      (log/error err))))


(defn- on-vt-change [evt]
  (on-time-change :crux.ui.time-type/vt evt))

(defn- on-tt-change [evt]
  (on-time-change :crux.ui.time-type/tt evt))

(def ^:private query-controls-styles
  [:style
   (garden/css
     [:.query-controls
      {:display         :flex
       :flex-direction  :column
       :justify-content :space-between
       :padding         "8px 24px"}
      [:&__item
       {:margin-bottom :16px}]
      [:&__item>label
       {:letter-spacing :.04em}]
      [:&__item>input
       {:padding       :4px
        :border-radius :2px
        :margin-top    :4px
        :border        "1px solid hsl(0, 0%, 85%)"}]])])


(defn root []
  [:div.query-controls
   query-controls-styles
   [:div.query-controls__item
    [:label "Valid Time (optional)"]
    [:input {:type "datetime-local" :name "vt" :on-change on-vt-change}]]
   [:div.query-controls__item
    [:label "Transaction Time (optional)"]
    [:input {:type "datetime-local" :name "tt" :on-change on-tt-change}]]])
