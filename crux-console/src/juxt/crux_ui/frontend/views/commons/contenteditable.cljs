(ns juxt.crux-ui.frontend.views.commons.contenteditable
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.views.commons.user-intents :as user-intents]))


(defn- -data-attrs-mapper [[k v]]
  (vector (str "data-" (name k)) v))

(defn- render-data-attrs [hmap]
  (into {} (map -data-attrs-mapper hmap)))


(defn div
  [id
   {:keys
    [on-change
     on-intent
     on-key-down
     on-blur
     on-change-complete
     text-mode?
     process-paste]
    :as opts}]
  ;;
  (let [node (r/atom nil)
        state-value (atom nil)

        get-cur-value
        (fn []
          (when-let [n @node]
            (if text-mode?
              (.-textContent n)
              (.-innerHTML n))))

        on-key-down (cond
                      on-intent #(some-> % user-intents/key-down-evt->intent-evt on-intent)
                      on-key-down on-key-down
                      :else identity)

        on-key-up-internal
        (if on-change
          (fn [evt]
            (let [cur-val (get-cur-value)]
              (when (not= cur-val @state-value)
                (on-change {:value  cur-val
                            :target @node})))))

        on-blur-internal
        (fn [evt]
          (if on-blur (on-blur))
          (if on-change-complete
            (on-change-complete
              {:value  (get-cur-value)
               :target @node})))

        on-paste-internal
        (if on-change
          (fn [evt]
            (let [cur-val (get-cur-value)]
              (when (not= cur-val @state-value)
                (let [paste-processed (if process-paste
                                        (process-paste cur-val)
                                        cur-val)]
                  (on-change {:value  paste-processed
                              :target @node}))))))]
    (r/create-class
      {:display-name "ContentEditable"

       :component-did-mount
                     (fn [this]
                       (reset! node (r/dom-node this)))

       :should-component-update
                     (fn [this cur-argv [f id next-props :as next-argv]]
                       (let [active-element js/document.activeElement
                             not-active?     (not= @node active-element)]
                         (and not-active? (not= (get-cur-value) (:value next-props)))))

       :reagent-render
                     (fn [id {:keys [data ; @param {map} with data attributes
                                     on-blur on-focus
                                     placeholder css-class value] :as opts}] ;; remember to repeat parameters
                       (reset! state-value value)
                       (let [id-str (if (keyword? id) (name id) (str id))
                             data-attrs (render-data-attrs data)]
                         [:div.crux-ui-input.contenteditable
                          (cond->
                            {:id                             id-str,
                             :suppressContentEditableWarning true
                             :dangerouslySetInnerHTML        {:__html value}
                             :placeholder                    placeholder
                             :class                          (if css-class (name css-class))
                             :autoFocus                      (:autofocus opts)
                             :content-editable               true
                             :spellCheck                     "false"
                             :on-key-up                      on-key-up-internal
                             :on-paste                       on-paste-internal
                             :on-key-down                    on-key-down
                             :on-focus                       on-focus
                             :on-blur                        on-blur-internal}
                            data-attrs (merge data-attrs))]))})))
