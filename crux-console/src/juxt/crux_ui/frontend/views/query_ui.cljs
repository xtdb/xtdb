(ns juxt.crux-ui.frontend.views.query-ui
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.cluster-health :as cluster-health]
            [juxt.crux-ui.frontend.views.query.form :as q-form]
            [juxt.crux-ui.frontend.views.query.output :as q-output]
            [juxt.crux-ui.frontend.views.query.time-controls :as time-controls]
            [juxt.crux-ui.frontend.subs :as sub]))

(def q-ui-border "1px solid hsl(0,0%,85%)")



(def query-ui-styles
  (garden/css
    [:.query-ui
      {:font-size :16px
       :border-radius :2px
       :margin "0 auto"
       :border q-ui-border
       :max-width :1600px
       :width :100%
       :height "calc(100% - 8px)"
       :margin-bottom :8px
      ;:max-height :100%
       :display :grid
       :place-items :stretch

       :grid-template
       "'output output' calc(100% - 330px)
       'controls form' 330px
        / minmax(200px, 300px) 1fr"}

      [:&__output
        {:padding "0px 0"
         :grid-area :output
         :border-bottom q-ui-border}]

      [:&__controls
        {:padding "16px 0"
         :grid-area :controls}]
        ;:border "1px solid orange"

      [:&__form
        {:padding "0px 0"
         :grid-area :form}]]

      (gs/at-media
        {:max-width :1000px}
        [:.query-ui
         {:grid-template
          "'output output' calc(100% - 330px)
          'form form' 330px
          / 1fr 1fr"
          :margin-bottom 0}
         [:&__controls
          {:display :none}]])))
        ;:border "1px solid green"



(defn query-ui []
  [:div.query-ui
   [:style query-ui-styles]
   [:div.query-ui__output
    [q-output/root]]
   [:div.query-ui__controls
    [time-controls/root]]
   [:div.query-ui__form
    [q-form/root]]])


(defn query [r]
  (let [q []
               ;conformed-query (s/conform :crux.query/query q)
               ;query-invalid? (= :clojure.spec.alpha/invalid conformed-query)
          ;start-time (System/currentTimeMillis)
          ;result (when-not query-invalid?
          ;         (api/q db q))
          ;query-time (- (System/currentTimeMillis) start-time)
          invalid? false; (and query-invalid? (not (str/blank? q)))
          on-cm-change-js "cm.save();"]
    [:div.query-editor {:style {:padding "2em"}}
     [cluster-health/root]]))
