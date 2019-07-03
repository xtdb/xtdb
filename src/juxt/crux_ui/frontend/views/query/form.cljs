(ns juxt.crux-ui.frontend.views.query.form
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.example-queries :as ex]
            [juxt.crux-ui.frontend.views.query.editor :as q-editor]
            [juxt.crux-ui.frontend.views.comps :as comps]))

(def ^:private -sub-query-input-malformed (rf/subscribe [:subs.query/input-malformed?]))
(def ^:private -sub-query-analysis (rf/subscribe [:subs.query/analysis]))
(def ^:private -sub-db-key (rf/subscribe [:subs.ui/editor-key]))

(defn- on-submit [e]
  (rf/dispatch [:evt.ui/query-submit]))

(defn btn []
  {:background    "hsl(190, 50%, 65%)"
   :color         "hsl(0, 0%, 100%)"
   :cursor        :pointer
   :border        0
   :padding       "12px 16px"
   :border-radius :2px})

(defn query-examples []
  [:div.examples
   (for [[ex-title ex-id] ex/examples]
     ^{:key ex-id}
     [:div.examples__item
      [comps/button-textual
       {:on-click #(rf/dispatch [:evt.ui.editor/set-example ex-id])
        :text ex-title}]])])


(def q-form-styles
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
        :height   :100%}

       [:&__editor
        {:height :100%}]

       [:&__editor-err
        :&__examples
        {:position :absolute
         :left     :32px
         :border-radius :2px
         :background :white
         :padding  :8px
         :bottom   :0px
         :color    "hsl(0,0%,50%)"
         :z-index  10}]


       [:&__type
        {:position :absolute
         :right    :8px
         :top      :8px
         :z-index  10}]

       [:&__submit
        {:position :absolute
         :right    :24px
         :display  :inline-block
         :bottom   :12px
         :z-index  10}]

       [:&__submit-btn
        (btn)
        {:background "hsla(190, 50%, 65%, .3)"}
        [:&:hover
         {:background "hsla(190, 50%, 65%, .8)"}]]]]))

(defn root []
  [:div.q-form
   [:style q-form-styles]
   [:div.q-form__editor
    ^{:key @-sub-db-key}
    [q-editor/root]]
   [:div.q-form__type (name (:crux.ui/query-type @-sub-query-analysis ""))]
   (if-let [e @-sub-query-input-malformed]
     [:div.q-form__editor-err
      "Query input appears to be malformed: " (.-message e)]
     [:div.q-form__examples
      [query-examples]])
   [:div.q-form__submit
    [:button.q-form__submit-btn {:on-click on-submit} "Run Query [ctrl + enter]"]]])

