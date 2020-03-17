(ns crux-ui.views.query-ui
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [crux-ui.views.query.form :as q-form]
            [crux-ui.views.query.output :as q-output]
            [crux-ui.views.style :as s]
            [crux-ui.views.functions :as vu]))


(def ^:private query-ui-styles
  [:style
   (garden/css
     [:.query-ui
      {:font-size :16px
       :border-radius :2px
       :margin "0 1rem 8px"
       :border s/q-ui-border
       :width :100%
       :overflow :hidden
       :height "calc(100% - 8px)"
       :display :grid
       :place-items :stretch
       :grid-template
       "'form output' 100% / minmax(512px, 38%) 62%"}
      [:&--query
       {:grid-template
        "'form output' 100% / 1fr 0px"}
       [">.query-ui__output"
        {:display :none}]]

      [:&--output
       {:grid-template
        "'form output' 100% / 0 100%"}
       [">.query-ui__form"
        {:display :none}]]

      [:&--horizontal
       {:grid-template
        "'output' calc(100% - 330px)
        'form' 330px"}]

      [:&__form
        {:padding "0px 0"
         :overflow :hidden
         :grid-area :form}]

      [:&--form-minimised
       {:grid-template
        "'form output' 100% / minmax(0px, 0%) 100%"}]

      [:&__output
        {:padding "0px 0"
         :grid-area :output
         :border-left s/q-ui-border}]]

     (gs/at-media {:max-width :1000px}
       [:.query-ui
        {:margin 0}]))])

(def ^:private mode->class-mod
  {:ui.display-mode/query "query"
   :ui.display-mode/output "output"})

(def -sub-display-mode (rf/subscribe [:subs.ui/display-mode]))
(def -sub-show-form (rf/subscribe [:subs.db.ui/show-form?]))

(defn query-ui []
  [:div
   (vu/bem :query-ui
           (if-not @-sub-show-form :form-minimised)
           (mode->class-mod @-sub-display-mode))
   query-ui-styles
   [:div.query-ui__form
    [q-form/root]]
   [:div.query-ui__output
    [q-output/root]]])
