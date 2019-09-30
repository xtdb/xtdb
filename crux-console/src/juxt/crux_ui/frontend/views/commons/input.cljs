(ns juxt.crux-ui.frontend.views.commons.input
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.views.commons.user-intents :as user-intents]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.style :as s]))


(defn- -data-attrs-mapper [[k v]]
  (vector (str "data-" (name k)) v))

(defn render-data-attrs [hmap]
  (into {} (map -data-attrs-mapper hmap)))


(def styles-src
  (list
    {:border         :none}
    {:border-bottom  s/q-ui-border}
    {:padding        "6px 0px"
     :border-radius  :2px
     :width          :100%
     :letter-spacing :.09em
     :outline        :none
     :font-size      :inherit
     :font-family    :inherit
     :display        :inline-block}
    ["&:empty"
     {:min-width :220px}]
    ["&:empty::before"
     {:content "attr(placeholder)"}]))

(def styles
  [:.crux-ui-input styles-src])


(defn text
  [id {:keys [on-change on-change-complete
              on-intent on-key-down
              value
              process-paste]
       :as opts}]
  (let [node          (r/atom nil)
        cur-val       (atom value)
        get-cur-value #(some-> @node (.-value))
        on-key-down   (cond
                        on-intent   #(some-> % user-intents/key-down-evt->intent-evt on-intent)
                        on-key-down on-key-down
                        :else       identity)

        on-blur-internal
        (fn [evt]
          (if on-change-complete
            (on-change-complete {:value (get-cur-value)})))

        on-key-up-internal
        (if on-change
          (fn [evt]
            (let [cur-value (get-cur-value)]
              (when (not= cur-value @cur-val)
                (on-change {:value  cur-value
                            :target @node})))))

        on-paste-internal
        (if on-change
          (fn [evt]
            (let [cur-html (get-cur-value)]
              (when (not= cur-html @cur-val)
                (let [paste-processed (if process-paste
                                        (process-paste cur-html)
                                        cur-html)]
                  (on-change {:value paste-processed
                              :target @node}))))))]
    (r/create-class
      {:display-name "SpaceInput"

       :component-did-mount
         (fn [this]
           (reset! node (r/dom-node this)))

       :component-did-update
         (fn [this old-argv]
           (let [nv (:value (r/props this))]
             (reset! cur-val nv)))

       :reagent-render
         (fn [id {:keys [data ; @param {map} with data attributes
                         on-blur on-focus
                         placeholder css-class value] :as opts}] ;; remember to repeat parameters
           (let [id-str (if (keyword? id) (name id) (str id))
                 attrs
                 (cond-> {:id           id-str
                          :placeholder  placeholder
                          :class        (if css-class (name css-class))
                          :defaultValue value
                          :autoFocus    (:autofocus opts)
                          :spellCheck   "false"
                          :on-key-up    on-key-up-internal
                          :on-paste     on-paste-internal
                          :on-key-down  on-key-down
                          :on-focus     on-focus
                          :on-blur      on-blur-internal}
                         (seq data) (merge (render-data-attrs data)))]

             ^{:key value}
             [:input.crux-ui-input attrs]))})))

