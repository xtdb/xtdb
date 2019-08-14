(ns juxt.crux-ui.frontend.views.commons.user-intents
  (:require [juxt.crux-ui.frontend.views.commons.keycodes :as kc]
            [juxt.crux-ui.frontend.views.commons.dom :as dom]))

(defn interpret-user-intent [target-value key-code]
  (if (empty? target-value)
    (case (kc/kc->kw key-code)
      ::kc/enter               ::create
      ::kc/backspace-or-delete ::delete
      nil)
    (case (kc/kc->kw key-code)
      ::kc/enter               ::create
      ::kc/backspace-or-delete nil
      nil)))

(defn key-down-evt->intent-evt
  "boolean altKey
   number charCode
   boolean ctrlKey
   boolean getModifierState(key)
   string key
   number keyCode
   string locale
   number location
   boolean metaKey
   boolean repeat
   boolean shiftKey
   number which"
  [react-evt]
  (let [t      (.-target react-evt)
        v      (or (not-empty (.-value t)) (.-innerHTML t))
        window-selection (.getSelection js/window)
        kc     (.-keyCode react-evt)
        intent (interpret-user-intent v kc)]
    (when intent
      (.preventDefault react-evt)
      {:intent intent
       :value  v
       :target t
       :data   (some-> t (dom/jsget "dataset") dom/dataset->clj-raw)})))
