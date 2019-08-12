(ns juxt.crux-ui.frontend.views.query.editor
  (:require [juxt.crux-ui.frontend.views.codemirror :as cm]
            [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query.examples :as query-examples]))

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
       {:padding "16px"
        :position :relative
        :max-height :100%
        :overflow :scroll
        :height :100%}

       [:&__query-type
        {:position :absolute
         :right    :8px
         :top      :8px
         :z-index  10}]

       [:&__err
        :&__examples
        {:position :absolute
         :left     :8px
         :max-width :80%
         :overflow :scroll
         :border-radius :2px
         :background :white
         :padding  :8px
         :bottom   :0px
         :color    "hsl(0,0%,50%)"
         :z-index  10}]])])


(defn root []
  [:div.q-editor
   query-ui-styles
   [:div.q-editor__query-type (name (:crux.ui/query-type @-sub-query-analysis ""))]
   (if-let [e @-sub-query-input-malformed]
     [:div.q-editor__editor-err
      "Query input appears to be malformed: " (.-message e)]
     [:div.q-editor__examples
      [query-examples/root]])
   ^{:key @-stats}
    [cm/code-mirror
     @-sub-query-input
     {:on-change on-qe-change
      :stats @-stats}]])

