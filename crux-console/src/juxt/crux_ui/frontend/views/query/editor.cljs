(ns juxt.crux-ui.frontend.views.query.editor
  (:require [juxt.crux-ui.frontend.views.codemirror :as cm]
            [garden.core :as garden]
            [re-frame.core :as rf]))

(def ^:private -sub-query-input (rf/subscribe [:subs.query/input]))
(def ^:private -stats (rf/subscribe [:subs.query/stats]))

(defn- on-qe-change [v]
  (rf/dispatch [:evt.ui/query-change v]))

(def query-ui-styles
  [:style
    (garden/css
      [:.query-editor
       {:height :100%}])])

(defn root []
  [:div.query-editor
   query-ui-styles
    ^{:key @-stats}
    [cm/code-mirror
     @-sub-query-input
     {:on-change on-qe-change
      :stats @-stats}]])

