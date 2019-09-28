(ns juxt.crux-ui.frontend.views.settings
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.input :as input]
            [juxt.crux-ui.frontend.views.commons.form-line :as fl]
            [juxt.crux-ui.frontend.subs]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.views.output.edn :as output-edn]))


(def ^:private -sub-settings (rf/subscribe [:subs.sys/settings]))

(def ^:private root-styles
  [:style
    (garden/css
      [:.settings
       {:padding "16px 32px"}
       [:&__line
        {:margin-top "40px"}]
       [:&__status
        {:margin-top "24px"}]])])

(defn- on-prop-change [prop-name {v :value :as change-complete-evt}]
  (rf/dispatch [:evt.db/prop-change {:evt/prop-name prop-name
                                     :evt/value v}]))

(defn- on-host-change [{v :value :as change-complete-evt}]
  (rf/dispatch [:evt.db/host-change v]))

(defn root []
  (let [{:keys [db.sys.host/status db.sys/host db.query/limit] :as s} @-sub-settings]
    ^{:key s}
    [:div.settings
     root-styles
     [:h1.settings__title "Settings"]
     [:div.settings__line
      [fl/line
       {:label "Query results limit"
        :control
        [input/text :ui.settings/qlimit
         {:on-change-complete (r/partial on-prop-change :db.query/limit)
          :value limit}]}]]
     [:div.settings__line
      [fl/line
       {:label "Crux HTTP-Server Host and port"
        :control
        [input/text :ui.settings/host
         {:on-change-complete on-host-change
          :value host}]}]
      [:pre.settings__status
       [:h3 "/status output"]
       [output-edn/simple-print status]]]]))


