(ns juxt.crux-ui.frontend.views.query.form
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.style :as s]
            [juxt.crux-ui.frontend.views.query.editor :as q-editor]
            [juxt.crux-ui.frontend.views.query.time-controls :as time-controls]
            [juxt.crux-ui.frontend.views.query.examples :as query-examples]
            [juxt.crux-ui.frontend.views.functions :as vu]))


(def ^:private -sub-editor-key (rf/subscribe [:subs.ui/editor-key]))
(def ^:private -sub-query-analysis (rf/subscribe [:subs.query/analysis]))

(defn- on-submit [e]
  (rf/dispatch [:evt.ui.query/submit {:evt/push-url? true}]))


(def col-base {:h 197 :s 80 :l 65 :a 0.8})

(def btn-color--base       (s/hsl (assoc col-base :a 0.5)))
(def btn-color--cta        (s/hsl col-base))
(def btn-color--cta-hover  (s/hsl (assoc col-base :a 0.9)))
(def btn-color--cta-active (s/hsl (assoc col-base :a 1)))



(defn btn-cta-styles [] ; todo move into comps
  {:background    btn-color--base
   :color         "white"
   :cursor        :pointer
   :border        0
   :letter-spacing "0.03em"
   :padding       "8px 14px"
   :border-radius :2px})

(def ^:private q-form-styles
  [:style
   (garden/css
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

      [:&__submit-btn
       (btn-cta-styles)
       [:&--cta
        {:background btn-color--cta}]
       [:&:hover
        {:background btn-color--cta-hover}]
       [:&:active
        {:background btn-color--cta-active}]]

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
        [time-controls/root]]
       (if @ex
         [:div.q-form__examples
          [query-examples/root]])
       [:div.q-form__submit
        (let [qa @-sub-query-analysis]
          [:button.q-form__submit-btn
           {:on-click on-submit
            :class (if qa "q-form__submit-btn--cta")}
           [:span "Run Query"]])
        #_[:small "[ctrl + enter]"]]])))

