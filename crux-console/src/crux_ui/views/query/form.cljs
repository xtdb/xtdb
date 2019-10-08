(ns crux-ui.views.query.form
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [crux-ui.views.style :as s]
            [crux-ui.views.query.editor :as q-editor]
            [crux-ui.views.query.time-controls :as time-controls]
            [crux-ui.views.commons.tiny-components :as comps]
            [crux-ui.views.query.examples :as query-examples]
            [crux-ui.views.functions :as vu]))


(def ^:private -sub-editor-key (rf/subscribe [:subs.ui/editor-key]))
(def ^:private -sub-query-analysis (rf/subscribe [:subs.query/analysis]))

(defn- on-submit [e]
  (rf/dispatch [:evt.ui.query/submit {:evt/push-url? true}]))


(def ^:private q-form-styles
  [:style
   (garden/css
     [:.examples-wrapper
      {:overflow :auto}]
     [:.q-form
      {:position :relative
       :display  :grid
       :overflow :hidden
       :grid-template
       "'editor editor' 1fr
        'time-controls submit' auto
        / 1fr 128px"
       :height   :100%}
      [:&--examples
       {:grid-template
        "'editor editor' 1fr
         'time-controls submit' auto
         'examples examples' auto
         / 1fr 128px"}]

      [:&__time-controls
       {:grid-area "time-controls"
        :padding "16px 8px 16px 16px"}]

      [:&__editor
       {:grid-area "editor"
        :overflow :hidden
        :height :100%}]

      [:&__submit
       {:grid-area :submit
        :text-align :center
        :padding "24px 8px"
        :place-self "end"}
       [:>small
        {:font-size :0.8em
         :color s/color-font-secondary}]]



      [:&__examples
       {:grid-area :examples
        :overflow :hidden
        :width "calc(100% - 32px)"
        :border-radius :2px
        :background :white
        :padding  "0px 0px"
        :position :relative
        :height :47px
        :left :16px
        :color    s/color-font-secondary
        :z-index  10}
       ["> .examples-wrapper"
        {:margin-left "-8px"
         :position :absolute
         :width :100%
         :top 0
         :padding-bottom :17px
         :overflow :scroll
         :bottom "-17px"}
        {:overflow-x :scroll
         :overflow-y :hidden}]
       ["> ::scrollbar"
        "> ::-moz-scrollbar"
        "> ::-webkit-scrollbar"
        {:display :none}]]]

     (gs/at-media {:max-width :1000px}
       [:.q-form
        {:padding-bottom :32px
         :grid-template
         "'editor' 1fr
          'time-controls' auto
          'submit' auto
           / 100%"}
        [:&--examples
         {:grid-template
          "'editor' 1fr
           'time-controls' auto
           'submit' auto
           'examples' auto
            / 100%"}]
        [:&__submit
          {:padding "0 16px"
           :justify-self :start}]]))])

(defn root []
  (let [ex (rf/subscribe [:subs.query/examples])]
    (fn []
      [:div (vu/bem :q-form  (if @ex :examples))
       q-form-styles
       [:div.q-form__editor
        ^{:key @-sub-editor-key}
        [q-editor/root]]
       [:div.q-form__time-controls
        [time-controls/root {:ui/layout :ui.layout/column}]]
       (if @ex
         [:div.q-form__examples
          [:div.examples-wrapper
           [query-examples/root]]])
       [:div.q-form__submit
        (let [qa @-sub-query-analysis]
          [comps/button-bordered
           {:on-click on-submit
            :css-mods ["bright" (if-not qa "inactive")]
            :text "Run Query"}])]])))
