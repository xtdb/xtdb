(ns juxt.crux-ui.frontend.views.output.attr-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]))

(def ^:private -sub-attr-history (rf/subscribe [:subs.output/attr-history]))

; History

; query yielding one entity
;  - txes on a scatter plot
;  - single / multiple attrs over time dynamic on a line chart


; query yielding multiple entities
;  - txes on a scatter plot, colored
;  - single attr history over time dynamic on a line chart for multiple entities

(def ^:private root-styles
  [:style
   (garden/css
     [:.attr-history
      {}])])

(defn root []
  [:div.attr-history])
