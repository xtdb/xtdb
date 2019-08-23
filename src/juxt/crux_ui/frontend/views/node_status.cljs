(ns juxt.crux-ui.frontend.views.node-status
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.input :as input]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.views.functions :as vf]
            [juxt.crux-ui.frontend.views.output.edn :as edn]
            [juxt.crux-ui.frontend.views.style :as s]))



(def ^:private -sub-node-addr (rf/subscribe [:subs.sys/host]))
(def ^:private -sub-node-status (rf/subscribe [:subs.sys.host/status]))

(defn- on-host-change [{v :value :as change-complete-evt}]
  (rf/dispatch [:evt.db/host-change v]))

(def ^:private node-styles
  [:style
   (garden/css
     [:.led
      (let [size :8px]
        {:height size
         :width size
         :background :grey
         :margin-right :8px
         :border-radius size})
      [:&--on
       {:background :green}]]
     [:.node
      {:display :flex
       :position :relative
       :justify-content :space-between
       :align-items :center
       :width "100%"}
      [:>.node__status
       {:display :none}]
      [:&:hover
        [:>.node__status
         {:position      :absolute
          :background    :white
          :padding :8px
         ;:max-width :500px
          :font-family "monospace"
          :display :block
          :z-index       100
          :font-size :1.2rem
          :border-radius :2px
          :border        s/q-ui-border
          :top           :32px}]]
      [:&__addr
       {:display "flex"
        :justify-content "space-between"
        :flex "0 0 250px"
        :align-items "center"}]])])


(defn led [on?]
  [:div (vf/bem :led {:on on? :off (not on?)})])

(defn node-status []
  [:div.node
   node-styles
   [:div.node__led [led (boolean @-sub-node-status)]]
   [:div.node__status
    (if-let [node-info @-sub-node-status]
      [:pre (f/pprint-str node-info)] ; alternatively [edn/root node-info]
      [:pre "Cannot connect to the specified node address"])]
   [:div.node__addr
    [input/text :ui.settings/host
     {:on-change-complete on-host-change
      :value @-sub-node-addr}]]])
