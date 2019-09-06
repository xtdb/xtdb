(ns juxt.crux-ui.frontend.views.query.form
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.style :as s]
            [juxt.crux-ui.frontend.views.query.editor :as q-editor]
            [juxt.crux-ui.frontend.views.query.time-controls :as time-controls]
            [juxt.crux-ui.frontend.views.query.examples :as query-examples]))


(def ^:private -sub-editor-key (rf/subscribe [:subs.ui/editor-key]))
(def ^:private -sub-query-analysis (rf/subscribe [:subs.query/analysis]))

(defn- on-submit [e]
  (rf/dispatch [:evt.ui.query/submit]))

(defn btn-cta-styles [] ; todo move into comps
  {:background    "hsl(190, 50%, 65%)"
   :color         "hsl(0, 0%, 100%)"
   :cursor        :pointer
   :border        0
   :padding       "12px 16px"
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
        'examples examples' auto
        / 1fr 128px"
       :height   :100%}

      [:&__time-controls
       {:grid-area "time-controls"
        :padding "16px 8px 16px 16px"}]

      [:&__editor
       {:grid-area "editor"
        :overflow :hidden
        :height :100%}]

      [:&__submit
       {:grid-area :submit
        :padding "0 8px 0 0"
        :place-self :center}]

      [:&__submit-btn
       (btn-cta-styles)
       {:background "hsla(190, 50%, 65%, .3)"}
       [:&--cta
        {:background "hsla(190, 50%, 65%, .8)"}]
       [:&:hover
        {:background "hsla(190, 60%, 65%, .9)"}]
       [:&:active
        {:background "hsla(190, 70%, 65%, 1.0)"}]
       [:>small
        {:font-size :0.8em}]]

      [:&__examples
       {:grid-area :examples
        :overflow :auto
        :width "calc(100% - 32px)"
        :border-radius :2px
        :background :white
        :padding  "0px 16px"
        :color    s/color-font-secondary
        :z-index  10}
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
          'examples' auto
           / 100%"}
        [:&__submit
          {:padding "0 16px"
           :justify-self :start}]]))])


(defn root []
  [:div.q-form
   q-form-styles
   [:div.q-form__editor
    ^{:key @-sub-editor-key}
    [q-editor/root]]
   [:div.q-form__time-controls
    [time-controls/root]]
   [:div.q-form__examples
    [query-examples/root]]
   [:div.q-form__submit
    (let [qa @-sub-query-analysis]
      [:button.q-form__submit-btn
       {:on-click on-submit
        :class (if qa "q-form__submit-btn--cta")}
       [:span "Run Query"][:br]
       [:small "[ctrl + enter]"]])]])

