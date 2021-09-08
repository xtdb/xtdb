(ns xtdb.ui.collapsible
  (:require [xtdb.ui.common :as common]
            [re-frame.core :as rf]))

(rf/reg-event-db
 ::toggle
 (fn [db [_ db-path open?]]
   (update-in db (into db-path [::open?]) (if (some? open?) (constantly open?) not))))

(rf/reg-sub
 ::open?
 (fn [db [_ db-path default-open?]]
   (get-in db (into db-path [::open?]) default-open?)))

(defn collapsible [db-path {:keys [default-open? label]} & body]
  (let [open? @(rf/subscribe [::open? db-path default-open?])]
    [:div.expand-collapse
     [:div.expand-collapse__group {:on-click #(rf/dispatch [::toggle db-path])}
      [:span.expand-collapse__txt
       [:span.form-pane__arrow
        [common/arrow-svg open?]
        label]]]

     (when open?
       (into [:div.expand-collapse__content] body))]))
