(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.common :as common]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [reitit.core :as reitit]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(defn vt-tt-inputs
  [{:keys [values handle-change handle-blur]}]
  [:div.crux-time
   [:div.input-group
    [:div.label
     [:label "Valid Time"]]
    [:input.input {:type "datetime-local"
                   :name "vt"
                   :value (get values "vt")
                   :on-change handle-change
                   :on-blur handle-blur}]]
   [:div.input-group
    [:div.label
     [:label "Transaction Time"]]
    [:input.input {:type "datetime-local"
                   :name "tt"
                   :value (get values "tt")
                   :on-change handle-change
                   :on-blur handle-blur}]]])

(defn query-form
  []
  (let [initial-values @(rf/subscribe [::sub/initial-values-query])]
    [fork/form {:form-id "form-query"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values initial-values
                :on-submit #(rf/dispatch [::events/go-to-query-view %])}
     (fn [{:keys [values
                  state
                  form-id
                  handle-change
                  handle-blur
                  handle-submit] :as props}]
       (let [loading? false]
         [:<>
          [:pre (with-out-str (pprint/pprint @state))]
          [:form
           {:id form-id
            :on-submit handle-submit}
           [:textarea.textarea.input-group__textarea
            {:name "q"
             :value (get values "q")
             :on-change handle-change
             :on-blur handle-blur
             :rows 10}]
           [vt-tt-inputs props]
           [:button.button
            {:type "submit"
             :disabled loading?}
            "Submit Entity"]]]))]))

(defn entity-form
  []
  (let [initial-values @(rf/subscribe [::sub/initial-values-entity])]
    [fork/form {:form-id "form-entity"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values initial-values
                :on-submit #(rf/dispatch [::events/go-to-entity-view %])}
     (fn [{:keys [values
                  form-id
                  state
                  handle-change
                  handle-blur
                  handle-submit] :as props}]
       (let [loading? @(rf/subscribe [::sub/entity-right-pane-loading?])]
         [:<>
          [:pre (with-out-str (pprint/pprint @state))]
          [:form
           {:id form-id
            :on-submit handle-submit}
           [:textarea.textarea.input-group__textarea
            {:name "eid"
             :value (get values "eid")
             :on-change handle-change
             :on-blur handle-blur}]
           [vt-tt-inputs props]
           [:button.button
            {:type "submit"
             :disabled loading?}
            "Submit Entity"]]]))]))

(defn form
  []
  (let [left-pane-view @(rf/subscribe [::sub/left-pane-view])]
    (if (= :query left-pane-view)
      [query-form]
      [entity-form])))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    [:<>
     (if error
       [:div.error-box error]
       [table/table data])]))

(defn query-view
  []
  (let [right-pane-view @(rf/subscribe [::sub/query-right-pane-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       {:class (if (= right-pane-view :table)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-right-pane-view :table])}
       "Table"]
      [:div.pane-nav__tab
       {:class (if (= right-pane-view :graph)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-right-pane-view :graph])}
       "Graph"]
      [:div.pane-nav__tab
       {:class (if (= right-pane-view :range)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-right-pane-view :range])}
       "Range"]]
     (case right-pane-view
       :table [query-table]
       :graph [:div "this is graph"]
       :range [:div "this is range"])]))

(defn- entity->hiccup
  [links edn]
  (if-let [href (get links edn)]
    [:a.entity-link
     {:href href}
     (str edn)]
    (cond
      (map? edn) (for [[k v] edn]
                   ^{:key (str (gensym))}
                   [:div.entity-group
                    [:div.entity-group__key
                     (entity->hiccup links k)]
                    [:div.entity-group__value
                     (entity->hiccup links v)]])
      (sequential? edn) [:ol.entity-group__value
                         (for [v edn]
                           ^{:key (str (gensym))}
                           [:li (entity->hiccup links v)])]
      (set? edn) [:ul.entity-group__value
                  (for [v edn]
                    ^{:key v}
                    [:li (entity->hiccup links v)])]
      :else (str edn))))

(defn entity-document
  []
  (let [{:keys [eid vt tt document document-no-eid linked-entities]}
        @(rf/subscribe [::sub/entity-right-pane-document])
        loading? @(rf/subscribe [::sub/entity-right-pane-loading?])]
    [:div.entity-map__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       [:<>
        [:div.entity-map
         (if document
           [:<>
            [:div.entity-group
             [:div.entity-group__key
              ":crux.db/id"]
             [:div.entity-group__value (str eid)]]
            [:hr.entity-group__separator]
            (entity->hiccup linked-entities document-no-eid)]
           [:<> [:strong eid] " entity not found"])]
        [:div.entity-vt-tt
         [:div.entity-vt-tt__title
          "Valid Time"]
         [:div.entity-vt-tt__value vt]
         [:div.entity-vt-tt__title
          "Transaction Time"]
         [:div.entity-vt-tt__value tt]]])]))

(defn entity-right-pane
  []
  (let [right-pane-view @(rf/subscribe [::sub/entity-right-pane-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       {:class (if (= right-pane-view :document)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-right-pane-view :document])}
       "Document"]
      [:div.pane-nav__tab
       {:class (if (= right-pane-view :history)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-right-pane-view :history])}
       "History"]]
     (case right-pane-view
       :document [entity-document]
       :history [:div "this is history"])]))

(defn left-pane
  []
  (let [left-pane-visible? @(rf/subscribe [::sub/left-pane-visible?])
        left-pane-view @(rf/subscribe [::sub/left-pane-view])]
    [:div.left-pane
     (if left-pane-visible?
       [:div.hide-button
        {:on-click #(rf/dispatch [::events/toggle-left-pane])}
        "Hide"]
       [:button.button.hidden-pane
        {:on-click #(rf/dispatch [::events/toggle-left-pane])}
        [:span "."]
        [:span "."]
        [:span "."]])
     [:div
      {:class (if left-pane-visible?
                "pane-toggled"
                "pane-untoggled")}
      [:div.pane-nav
       [:div.pane-nav__tab
        {:class (if (= left-pane-view :query)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-left-pane-view :query])}
        "Query"]
       [:div.pane-nav__tab
        {:class (if (= left-pane-view :entity)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-left-pane-view :entity])}
        "Entity"]]
      [form]]]))

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:<>
     [:pre (with-out-str (pprint/pprint @(rf/subscribe [:db])))]
     [:div.container.page-pane
      (when name [left-pane])
      [:div.right-pane
       [:div.back-button
        [:a
         {:on-click common/back-page}
         [:i.fas.fa-chevron-left]
         [:span.back-button__text "Back"]]]
       (case name
         :query [query-view]
         :entity [entity-right-pane]
         [:div "no matching"])]]]))
