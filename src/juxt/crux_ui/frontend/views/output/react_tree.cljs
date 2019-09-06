(ns juxt.crux-ui.frontend.views.output.react-tree
  (:require ["react-ui-tree" :as ReactTree]
            [garden.core :as garden]))

(defn on-tree-change [evt]
  (println :on-tree-change evt))

(def style
  [:style
   (garden/css
     [:.react-tree
      {:min-width :100px
       :min-height :100px
       :height :100%
       :overflow :auto
       :padding "16px 24px"}])])

(defn root [tree-struct]
  [:div.react-tree
   style
   [:> ReactTree
     {:paddingLeft 20
      :onChange on-tree-change
      :renderNode
      (fn [node]
        (.-title node))

      :tree (clj->js tree-struct)}]])

