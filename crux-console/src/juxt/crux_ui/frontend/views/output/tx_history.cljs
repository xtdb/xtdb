(ns juxt.crux-ui.frontend.views.output.tx-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]))

(def ^:private -sub-tx-history (rf/subscribe [:subs.output/tx-history]))

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
     [:.tx-history
      {}])])

(defn root []
  [:div.tx-history])
