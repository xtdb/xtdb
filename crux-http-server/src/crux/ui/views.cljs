(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as string]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(defn query-box
  []
  (let [now-date (t/date)
        now-time (t/time)]
    [fork/form {:path :query
                :form-id "query"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values {"valid-date" now-date
                                 "valid-time" now-time}
                :on-submit #(rf/dispatch [::events/go-to-query-table %])}
     (fn [{:keys [values
                  state
                  form-id
                  handle-change
                  handle-blur
                  submitting?
                  handle-submit]}]
       [:div
        [:form
         {:id form-id
          :on-submit handle-submit}
         [:textarea.textarea
          {:name "q"
           :value (get values "q")
           :on-change handle-change
           :on-blur handle-blur
           :cols 40
           :rows 10}]
         [:div.crux-time
          [:div.input-group.valid-time
           [:div.label
            [:label "Valid Time"]]
           [:input.input {:type "date"
                          :name "valid-date"
                          :value (get values "valid-date")
                          :on-change handle-change
                          :on-blur handle-blur}]
           [:input.input {:type "time"
                          :name "valid-time"
                          :step "any"
                          :value (get values "valid-time")
                          :on-change handle-change
                          :on-blur handle-blur}]]
          [:div.input-group
           [:div.label
            [:label "Transaction Time"]]
           [:input.input {:type "date"
                          :name "transaction-date"
                          :value (get values "transaction-date")
                          :on-change handle-change
                          :on-blur handle-blur}]
           [:input.input {:type "time"
                          :name "transaction-time"
                          :value (get values "transaction-time")
                          :step "any"
                          :on-change handle-change
                          :on-blur handle-blur}]]]
         [:button.button
          {:type "submit"}
          "Submit Query"]]])]))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    (if error
      [:div.error-box error]
      [table/table data])))

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
    [:a {:href href}
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
  (let [{:keys [linked-entities entity-result]}
        @(rf/subscribe [::sub/entity-view-data])]
    [:<>
     [:h1 "/_entity"]
     [:div (entity->hiccup linked-entities entity-result)]]))

(defn view []
  (let [current-page @(rf/subscribe [::sub/current-page])]
    [:div
     (case (:handler current-page)
       :query [query-view]
       :entity [entity-view]
       [:div "no matching"])]))
