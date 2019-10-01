(ns crux-ui.views.style)

(def color-placeholder "grey")
(def color-font-secondary "hsl(0,0%,55%)")
(def q-ui-border "1px solid hsl(0,0%,85%)")

(defn hsl
  ([{:keys [h s l a] :as conf}]
   (if a
     (str "hsla(" h "," s "%," l "%," a ")")
     (str "hsl(" h "," s "%," l "%)")))
  ([h s l]
   (str "hsl(" h "," s "%," l "%)"))
  ([h s l a]
   (str "hsl(" h "," s "%," l "%," a ")")))
