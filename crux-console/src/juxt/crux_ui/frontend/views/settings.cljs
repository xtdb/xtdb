(ns juxt.crux-ui.frontend.views.settings
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.input :as input]
            [juxt.crux-ui.frontend.views.commons.form-line :as fl]
            [juxt.crux-ui.frontend.subs]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.views.output.edn :as output-edn]
            [juxt.crux-ui.frontend.views.commons.tiny-components :as comps]))


(def ^:private root-styles
  [:style
    (garden/css
      [:.g-spacer-w-40
       {:height :16px
        :display :inline-block
        :width :40px}]
      [:.g-container
       {:height :100%
        :overflow :hidden}
       [:&__content
        {:overflow :auto
         :height :100%}]]
      [:.g-sticky
       {:position :sticky
        :top :0px}]
      [:.settings
       {:padding "0px 32px 32px"}
       [:&__title
        {:padding-top :16px
         :background :white}]
       [:&__line
        {:margin-top "40px"}
        [:&--submit
         {}]]
       [:&__status
        {:margin-top "24px"}
        [:>h3
         {:font-weight 400
          :font-size :20px
          :margin-bottom :8px}]]])])

#_(defn- on-prop-change [prop-name {v :value :as change-complete-evt}]
    (rf/dispatch [:evt.db/prop-change {:evt/prop-name prop-name
                                       :evt/value v}]))

(defn- on-host-change [{v :value :as change-complete-evt}]
  (rf/dispatch [:evt.db/host-change v]))

(defn root []
  (let [-sub-settings (rf/subscribe [:subs.sys/settings])
        -local-atom (r/atom @-sub-settings)
        on-prop-change
        (fn on-prop-change [prop-name {v :value :as change-complete-evt}]
          (swap! -local-atom prop-name v))]
    (fn []
      (let [{:keys [db.sys.host/status ] :as s} @-local-atom]
        [:div.settings
         root-styles
         [:h1.settings__title.g-sticky "Settings"]
         [:div.settings__line
          [fl/line
           {:label "Query results limit"
            :control
            [input/text :ui.settings/qlimit
             {:on-change-complete (r/partial on-prop-change :db.query/limit)
              :value (:db.query/limit @-local-atom)}]}]]

         [:div.settings__line
          [fl/line
           {:label "Crux HTTP-Server Host and port"
            :control
            [input/text :ui.settings/host
             {:on-change-complete on-host-change
              :value (:db.sys/host @-local-atom)}]}]
          [:pre.settings__status
           (let [s (:db.sys.host/status @-local-atom)]
             (if status
               [:<>
                [:h3 "connection successful"]
                [output-edn/simple-print status]]
               [:h3 "connection failed"]))]]

         [:div.settings__line.settings__line--submit
          [comps/button-cta
           {:text "Save and quit"}]
          [:div.g-spacer-w-40]
          [comps/button-textual
           {:text "Cancel"}]]]))))




