(ns juxt.crux-ui.frontend.views.commons.form-line
  (:require [clojure.string :as s]))

(defn line [{:keys [on-reset css-class label control hint]}]
  [:div.line {:class css-class :title label}
   (if label
     [:div.line__label label])
   [:div.line__control control
    (if on-reset
      [:button.line__reset
       {:title    (if-not label
                    "reset"
                    (s/join " " (cons "reset" (filter string? label))))
        :on-click on-reset} "x"])]
   (if hint
     [:div.line__hint  hint])])
