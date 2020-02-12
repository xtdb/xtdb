(ns crux-ui.views.commons.dom
  (:require [clojure.string :as s]
            [cljs.reader :as reader]
            [reagent.core :as r]
            [crux-ui.functions :as f]
            [crux-ui.views.commons.keycodes :as kc]))

(def window js/window)
(def doc js/document)
(def str->id js/parseInt)
(def jsget goog.object/getValueByKeys)

(defn- calc-initial-host []
  (let [hs js/location.hostname
        doc js/document.documentElement
        conf-base-url (not-empty (jsget doc "dataset" "cruxBaseUrl"))
        conf-port (not-empty (jsget doc "dataset" "cruxHttpPort"))]
    (or conf-base-url
        (case hs
          "localhost" (str "localhost" (if conf-port (str ":" conf-port)))
          (str hs "/crux")))))

(defn- get-routes-prefix []
  (not-empty (jsget js/document.documentElement "dataset" "routesPrefix")))

(defn- gid [id] (js/document.getElementById id))

(defn get-body-width []
  (.-width (.getBoundingClientRect js/document.body)))

(defn evt->keycode-kw [react-evt]
  (kc/kc->kw (^js .-keyCode react-evt)))

(defn- -dispatch-on-keycode [dipatch-map evt]
  (when-let [f (get dipatch-map (evt->keycode-kw evt))]
    (^js .preventDefault evt)
    (f evt)))

(defn dispatch-on-keycode [dispatch-map]
  (r/partial -dispatch-on-keycode dispatch-map))

(defn get-elem-pos [elem]
  (let [rect (.getBoundingClientRect elem)]
    {:width  (.-width rect)
     :height (.-height rect)
     :top    (.-top rect)
     :left   (.-left rect)}))

(defn elem-height [elem]
  (:height (get-elem-pos elem)))

(defn get-window-scroll []
  (or (.-pageYOffset window)
      (-> doc .-documentElement .-scrollTop)
      (-> doc .-body .-scrollTop)
      0))

(defn scroll-to! [scroll]
  (let [res-scroll-y (- scroll (get-window-scroll))]
    (.scrollBy window 0 res-scroll-y)))

(defn scroll-by! [scroll]
  (.scrollBy window 0 scroll))

(defn get-viewport-height []
  js/window.innerHeight)

(defn get-scroll-root-height []
  (let [cal-elem (gid "cal")]
    (or 10000
        (some-> js/document.documentElement (jsget "dataset" "height") js/parseInt)
        (elem-height cal-elem))))

(defn calc-scroll-top-for-vertical-center []
  (/ (- (get-scroll-root-height) js/window.innerHeight) 2))

(defn re-center-vertically! []
  (println "re-center-vertically")
  (scroll-to! (+ 0 (calc-scroll-top-for-vertical-center))))

(defn calc-evt-path-js [evt]
  (or (.-path evt)
      (and (.-composedPath evt)
           (.composedPath evt))))

(defn has-class? [elem class-name]
  (some-> elem (.-classList) (.contains class-name)))

(defn evt-has-in-path? [evt class-name]
  (let [path (-> evt (jsget "nativeEvent") calc-evt-path-js js->clj)]
    (some #(has-class? % class-name) path)))


(defn get-container-dimensions []
  {:body-width         (get-body-width)
   :container-height   js/window.innerHeight
   :container-width    js/window.innerWidth
   :scroll-cont-height (get-scroll-root-height)})

(defn get-element-by-id [id]
  (js/document.getElementById id))

(defn parse-elem-eid [elem]
  (some-> elem (jsget "dataset" "entityId") str->id))

(defn evt->entity-id [evt]
  (some-> evt (jsget "currentTarget" "dataset" "entityId") str->id))

(defn evt->data-idx [evt]
  (some-> evt (jsget "currentTarget" "dataset" "idx") js/parseInt))


(defn select-one [sel & [ctx]]
  (.querySelector (or ctx js/document) sel))

(defn select-all [sel & [ctx]]
  (.querySelectorAll (or ctx js/document) sel))

(defn get-target-text [evt]
  (.. evt -currentTarget -textContent))

(defn get-target-value [evt]
  (.. evt -currentTarget -value))

(defn parse-data-id [elem]
  (if elem
    (js/parseInt (.. elem -dataset -id))))

(defn- -data-attrs-mapper [[k v]]
  (vector (str "data-" (name k)) v))

(defn render-data-attrs [hmap]
  (into {} (map -data-attrs-mapper hmap)))

(defn- camel-dash-replace [match]
  (str "-" (.toLowerCase (first match))))

(defn closest-parent [elem css-class]
  (if-not elem
    nil
    (if (has-class? elem css-class)
      elem
      (recur (.-parentElement elem) css-class))))

(defn camel->dashes
  "Convert camelCase identifier string to hyphen-separated keyword."
  [id]
  (s/replace id #"[A-Z]" camel-dash-replace))

(defn dataset->clj-raw [ds]
  (let [keys (js->clj (.keys js/Object ds))]
    (persistent!
      (reduce (fn [mem key]
                (assoc! mem (keyword (camel->dashes key)) (aget ds key)))
              (transient {})
              keys))))

(defn parse-int-or-nil [v]
  (let [x (js/parseInt v)]
    (if (js/isNaN x) nil x)))

(defn parse-entity-id [elem]
  (parse-int-or-nil (jsget elem "dataset" "entityId")))

(defn parse-evt-entity-id [evt]
  (-> evt (jsget "currentTarget" "dataset" "entityId") parse-int-or-nil))

(defn try-parse-int [str]
  (let [parse-res (js/parseInt str)]
    (if (js/isNaN parse-res)
      str
      parse-res)))

(defn read-number-or-nil [str]
  (try
    (let [read-res (reader/read-string str)]
      (if (number? read-res)
        read-res
        nil))
    (catch js/Error e
      nil)))

(defn read-keyword-or-nil [str]
  (try
    (let [read-res (reader/read-string str)]
      (if (keyword? read-res)
        read-res
        str))
    (catch js/Error e
      str)))

(defn autoparse [str]
  (cond
    (#{"null" "nil"} str) nil
    (empty? str) nil
    :else
    (or
      (read-number-or-nil str)
      (read-keyword-or-nil str)
      str)))
;(autoparse "aaa:2323")

(defn dataset->clj [dom-ds]
  (f/map-values autoparse (dataset->clj-raw dom-ds)))

(defn event->target-data [evt]
  (dataset->clj (.. evt -currentTarget -dataset)))
