(ns juxt.crux-ui.frontend.views.query.editor
  (:require [juxt.crux-ui.frontend.views.codemirror :as cm]
            [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query.examples :as query-examples]
            [juxt.crux-ui.frontend.views.style :as s]
            [garden.stylesheet :as gs]))

(def ^:private -sub-query-input (rf/subscribe [:subs.query/input]))
(def ^:private -sub-query-analysis (rf/subscribe [:subs.query/analysis]))
(def ^:private -stats (rf/subscribe [:subs.query/stats]))
(def ^:private -sub-query-input-malformed (rf/subscribe [:subs.query/input-malformed?]))

(defn- on-qe-change [v]
  (rf/dispatch [:evt.ui/query-change v]))

(def query-ui-styles
  [:style
    (garden/css
      [:.q-editor
       {:padding "8px"
        :position :relative
        :max-height :100%
       ;:overflow :scroll
        :height :100%}

       [:&__query-type
        {:position :absolute
         :right    :8px
         :color    s/color-font-secondary
         :top      :8px
         :z-index  10}]

       [:&__error
        {:position :absolute
         :color :grey
         :background :white
         :bottom :8px
         :z-index 10
         :left :24px}]])])




(defn root []
  [:div.q-editor
   query-ui-styles
   [:div.q-editor__query-type (name (:crux.ui/query-type @-sub-query-analysis ""))]
   (if-let [e @-sub-query-input-malformed]
     [:div.q-editor__error
      "Query input appears to be malformed: " (.-message e)])

   ^{:key @-stats}
    [cm/code-mirror
     @-sub-query-input
     {:on-change on-qe-change
      :stats @-stats}]])

