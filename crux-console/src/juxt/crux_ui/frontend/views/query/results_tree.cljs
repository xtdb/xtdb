(ns juxt.crux-ui.frontend.views.query.results-tree
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [clojure.string :as str]
            [stylefy.core :as stylefy :refer [use-style]]
            ))

(def us use-style)

(stylefy/init)

(defn visit [m visitors]
  ;; [node node-s next-s down-s right-s get-right get-down init-right-s cut-down cut-right :as m]
  ;; {:keys [break]} next-s
  ;; ;use next-state by default, unless down or right is necessary
  (let
    [dovisit (fn [m dir]
               (loop [s m [v & vs] visitors]
                 (if v (recur (apply v (concat [dir] s)) vs) s)))
     m2 (dovisit m :pre)
     ;break/cut checks should be between each visitor (:pre :post and :in) according to zip-visit
     ;https://github.com/akhudek/zip-visit/blob/master/src/zip/visit.cljc#L5
     ;(if (or b c)
     ;     {:node node* :state state* :break b :cut c}
     m3
     (let [[node* _ next-s* _ _ _ get-down* init-right-s* cut-down _] m2]
       (if-let [down* (and (not cut-down)
                           (not (:break next-s*))
                           (get-down* node*))]
         (assoc m2 2 (nth
                       (visit
                         (assoc m2
                                0 down*
                                1 nil
                                4 init-right-s*)
                         visitors) 2)
                   8 false)
         (assoc m2 8 false)))
     m4 (dovisit m3 :mid)
     m5
     (let [[_ _ _ down-s _ _ _ _ _ _] m
           [node* _ next-s* _ _ get-right* _ _ _ cut-right] m3]
       (if-let [right* (and (not cut-right)
                            (not (:break next-s*))
                            (get-right* node*))]
                (let [mm
                       (visit
                         (assoc m4
                                0 right*
                                1 nil
                                3 down-s)
                         visitors)]
         (assoc m4 2 (nth mm 2)
                   9 false))
         (assoc m4 9 false)))
    m6 (dovisit m5 :post)]
    m6
    ;:in (needs get-up, such that an :in visitor can't replace the node), :in happens after post, it's about operations in the context of the parent between each child
    ))

(defn parent-styleg [cmax]
  {:padding "4rem"
   :display "grid"
   :grid-template-columns (str/join " " (concat ["auto"] (repeat (inc cmax) "2rem") ["auto"]))})

(defn divn [r c] {:align-content "center"
                  :line-height "1.8rem"
                  :position "relative"
                  ;:background-color "white"
                  :grid-area (str/join " / " (map #(inc %) [r (inc c) (inc r) -2]))})

(defn divnb [r c i] {:align-content "center"
                  :line-height "1.8rem"
                  :position "relative"
                  :border-right (if (= i 0) "" "1px dashed lightgrey")
                  :border-top (if (= i 0) "" "1px solid grey")
                  :z-index "-1"
                  :grid-area (str/join " / " (map #(inc %) [(inc r) (inc c) (+ (inc r) i) (inc (inc c))]))
                  :margin-right "-1px"
                  })

(def tree
  ["hi there"
   ["hello!"
    ["yup!"
     ["other";"what about this and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this  and this "
      ["and this"]]
     ["or other"
      ["or this"]]
     ["yes"
     ["or this?"
      ["another"
       ["and another"]]]]]]
   ["new user"
    ["I N T E R F A C E"]]])


(def get-right #(when (some? (first (:right %))) {:d (first (:right %)) :right (rest (:right %))}))
(def get-down #(when (some? (second (:d %))) {:d (second (:d %)) :right (drop 2 (:d %))}))

;TODO extract generic counting-visitor using ::count/<keyword>

(defn tree-data [tree]
  (nth (visit [{:d tree
                  :right nil}
                  nil
                  {:i 0 :v []} {:d 0} {:acc 0}
                  get-right
                  get-down
                 {:acc 0}
                 false
                 false]
                [(fn [dir node node-s next-s down-s right-s get-right get-down init-right-s cut-down cut-right]
                   (case dir
                     :pre
                     [node
                      (:i next-s)
                      (assoc next-s
                             :v (conj (:v next-s)
                                      {:r (:i next-s)
                                       :c (:d down-s)
                                       :t (first (:d node))})
                             :i (inc (:i next-s))
                             :countc (max (:countc next-s) (:d down-s))
                             :last-child (if (nil? (get-right node))
                                           (:i next-s)
                                           (:last-child next-s)))
                      (assoc down-s :d (inc (:d down-s)))
                      (assoc right-s :acc (+ (:acc right-s) 1))
                      get-right get-down init-right-s cut-down cut-right]
                     :mid
                     [node node-s
                      (assoc-in next-s [:v node-s :h]
                                (if (nil? (:last-child next-s))
                                  0
                                  (- (:last-child next-s) (get-in next-s [:v node-s :r]))))
                      down-s
                      right-s
                      get-right get-down init-right-s cut-down cut-right]
                     :post
                     [node node-s
                      (assoc next-s
                             :last-child
                             (if (nil? (get-right node)) (get-in next-s [:v node-s :r]) (or (:last-child next-s) 0))
                             )
                      down-s
                      right-s
                      get-right get-down init-right-s cut-down cut-right]))]
                ) 2))


(defn tree-ui [tree-data]
;  (println (map #(get (:v tree-data) %) (keys (:v tree-data))))
  (apply (partial conj [:div (us (parent-styleg (:countc tree-data)))])
      (mapcat identity (for [{:keys [r c h t]} (:v tree-data)]
;(reverse (map #(get (:v tree-data) %) (keys (:v tree-data))))
        [[:div (us (divnb r c h)) ""] (println h)
         [:div (us (divn r c)) (if (= r 3) [:span (us {:position "absolute" :font-size "0.6rem" :transform "translateX(calc(-100% - 0.2rem))"}) "▶"]) [:span (us (merge {:box-decoration-break "clone" :-webkit-box-decoration-break "clone" :padding "0.32rem" :text-align "justify" :text-align-last "left" :border-left "1px solid grey"
                                                                                                                                                     #_( ::stylefy/mode {:after {:content "''" :width (if true "2rem" "100%") :height "0px" :position "absolute" :left "0" :bottom "0" :border-bottom "1px solid grey"}}
                                                                                                                                                                        )
                                                                                                                                                      } (if (= r 3) {:background-color "#f3f3f3"} {})))
                                                                                                                                                       t
                                                                                                                                                       ;(str/join " " [r c h t])
                                                                                                                                                       ]]]))))



(def ^:private -sub-results-table (rf/subscribe [:subs.query/results-table]))
(def col-border "hsl(0, 0%, 85%)")

(def style
  (garden/css
    [:.q-tree
     {:width "100%"
      :height :100%
     ;:border (str "1px solid " col-border)
      :border-radius :2px}
     ]))

; TODO tree doesn't display correctly using this as tree-data (visitor logic problem!)
#_(println ["Results:"
              [":test2"
               [":crux.db/id"]
               [":first-name"]
               [":last-name"]
               [":name"]]
              [":test"
               [":crux.db/id"]
               [":first-name"]
               [":last-name"]
               [":name"]]
              [":test3"
               [":crux.db/id"]
               [":first-name"]
               [":last-name"]
               [":name"]]])

(defn root []
  (let [{:keys [headers rows]} @-sub-results-table]
    (println headers rows)
    [:div.q-tree
     [:style style]
     (tree-ui
     (tree-data (into ["Results:"]
                      (map (fn [r] (concat [(str (first r))] (drop 1 (map-indexed #(do [(str %2) [(nth r %1)]]) headers)))) rows)))
       )
     ;(tree-data tree))
]
    ))
