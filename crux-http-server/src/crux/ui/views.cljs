(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as subs]
   [reagent.core :as r]
   [crux.ui.uikit.table :as table]
   [re-frame.core :as rf]))

(defn query-box
  []
  (let [query-value (r/atom nil)]
    (fn []
      [:div
       [:form
        {:on-submit #(do
                       (.preventDefault %)
                       (rf/dispatch [::events/submit-query-box @query-value]))}
        [:textarea.textarea
         {:name "q"
          :value @query-value
          :on-change #(reset! query-value (-> % .-target .-value))
          :cols 40
          :rows 10}]
        [:br]
        [:br]
        [:button.button
         {:type "submit"}
         "Submit Query"]]])))

(defn query-table
  []
  (let [query-data @(rf/subscribe [::subs/query-data])]
    [table/table query-data]))

(defn query-view
  []
  (rf/dispatch [::events/inject-metadata :query-data])
  (fn []
    (let [{:keys [query-params]} @(rf/subscribe [::subs/current-page])]
      (if (seq query-params)
        [query-table]
        [query-box]))))

(defn view []
  (let [current-page @(rf/subscribe [::subs/current-page])]
    [:div
     (case (:handler current-page)
       :query [query-view]
       [:div "no matching"])
#_     [:pre
      (with-out-str
        (pprint/pprint @(rf/subscribe [:db])))]]))
