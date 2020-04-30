(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as sub]
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
                       (rf/dispatch [::events/go-to-query-table @query-value]))}
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
  (rf/dispatch [::events/fetch-query-table])
  (fn []
    (let [query-data-table @(rf/subscribe [::sub/query-data-table])]
      [table/table query-data-table])))

(defn query-view
  []
  (let [{:keys [query-params]} @(rf/subscribe [::sub/current-page])]
    [:<>
     [:h1 "/_query"]
     (if (seq query-params)
       [query-table]
       [query-box])]))

(defn- entity->hiccup
  [links edn]
  (if-let [href (get links edn)]
    [:a {:href href
         :on-click #(rf/dispatch [::events/fetch-entity])}
     (str edn)]
    (cond
      (map? edn) (into [:dl]
                       (mapcat
                        (fn [[k v]]
                          [[:dt (entity->hiccup links k)]
                           [:dd (entity->hiccup links v)]])
                        edn))
      (sequential? edn) (into [:ol] (map (fn [v] [:li (entity->hiccup links v)]) edn))
      (set? edn) (into [:ul] (map (fn [v] [:li (entity->hiccup links v)]) edn))
      :else (str edn))))

(defn entity-view
  []
  (rf/dispatch [::events/fetch-entity])
  (fn []
    (let [{:keys [linked-entities entity-result]}
          @(rf/subscribe [::sub/entity-view-data])]
      [:<>
       [:h1 "/_entity"]
       [:div (entity->hiccup linked-entities entity-result)]])))

(defn view []
  (let [current-page @(rf/subscribe [::sub/current-page])]
    [:div
     (case (:handler current-page)
       :query [query-view]
       :entity [entity-view]
       [:div "no matching"])
     #_[:pre
      (with-out-str
        (pprint/pprint @(rf/subscribe [:db])))]]))
