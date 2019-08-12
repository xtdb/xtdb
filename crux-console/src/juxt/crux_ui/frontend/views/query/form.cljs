(ns juxt.crux-ui.frontend.views.query.form
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.style :as s]
            [juxt.crux-ui.frontend.views.query.editor :as q-editor]
            [juxt.crux-ui.frontend.views.query.examples :as query-examples]
            [juxt.crux-ui.frontend.views.query.time-controls :as time-controls]))


(def ^:private -sub-editor-key (rf/subscribe [:subs.ui/editor-key]))

(defn- on-submit [e]
  (rf/dispatch [:evt.ui/query-submit]))

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
     [[:.examples
       {:display :flex}
       [:&__item
        {:padding :8px}
        [:&:hover
         {:color :black}]]]
      (gs/at-media {:max-width :1000px}
        [:.examples
         {:display :none}])

      [:.q-form
        {:position :relative
         :display  :grid
         :overflow :hidden
         :grid-template
         "'time-controls editor' / minmax(280px, 400px) 1fr"
         :height   :100%}

        [:&__time-controls
         {:grid-area "time-controls"
          :border-right s/q-ui-border}]

        [:&__editor
         {:grid-area "editor"
          :overflow :hidden
          :height :100%}]

        [:&__submit
         {:position :absolute
          :right    :24px
          :display  :inline-block
          :bottom   :12px
          :z-index  10}]

        [:&__submit-btn
         (btn-cta-styles)
         {:background "hsla(190, 50%, 65%, .3)"}
         [:&:hover
          {:background "hsla(190, 50%, 65%, .8)"}]]

        (gs/at-media {:max-width :1000px}
          [:.q-output
           {:grid-template "'editor editor' 1fr / minmax(280px, 400px) 1fr"}
           [:&__side
            {:display :none}]])]])])


(defn root []
  [:div.q-form
   q-form-styles
   [:div.q-form__time-controls
    [time-controls/root]]
   [:div.q-form__editor
    ^{:key @-sub-editor-key}
    [q-editor/root]]
   [:div.q-form__submit
    [:button.q-form__submit-btn {:on-click on-submit} "Run Query [ctrl + enter]"]]])

