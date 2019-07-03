(ns juxt.crux-ui.frontend.views.codemirror
  (:require [reagent.core :as r]
            [garden.core :as garden]
            [goog.object :as gobj]
            ["/codemirror/lib/codemirror.js" :as codemirror]
            ["/codemirror/mode/clojure/clojure.js"]
            ["/codemirror/addon/edit/closebrackets.js"]
            ["/codemirror/addon/edit/matchbrackets.js"]
            ["/codemirror/addon/hint/show-hint"]
            ["/codemirror/addon/hint/anyword-hint"]
            ))


(def code-mirror-styling
  (garden/css
    [[:.code-mirror-container
       {:font-size :17px
        :padding "0px 0px"
        :height :100%}]
     [:.CodeMirror
      {:border-radius :2px
      ;:border "1px solid hsl(0, 0%, 90%)"
       :height :100%
       }]]))

(defn escape-re [input]
  (let [re (js/RegExp. "([.*+?^=!:${}()|[\\]\\/\\\\])" "g")]
    (-> input str (.replace re "\\$1"))))

(defn fuzzy-re [input]
  (-> (reduce (fn [s c] (str s (escape-re c) ".*")) "" input)
      (js/RegExp "i")))

(defn autocomplete [index cm options]
  (let [cur    (.getCursor cm)
        line   (.-line cur)
        ch     (.-ch cur)
        token  (.getTokenAt cm cur)
        reg    (subs (.-string token) 0 (- ch (.-start token)))
        blank? (#{"[" "{" " " "("} reg)
        start  (if blank? cur (.Pos codemirror line (gobj/get token "start")))
        end    (if blank? cur (.Pos codemirror line (gobj/get token "end")))
;        words  (->> (cm-completions index cm) (mapv first))
        words (concat [:find :where :args :rules :offset :limit :order-by
                       :timeout :full-results? :not :not-join :or :or-join
                       :range :unify :rule :pred] index)]
    (if words
      (let [fuzzy (if blank? #".*" (fuzzy-re reg))]
        (clj->js {:list ;index
             (->> words
                        (map str)
                        (filter #(re-find fuzzy %))
                        ;sort
                        clj->js)
             :from start
             :to   end
             })))))

(defn code-mirror
  [initial-value {:keys [read-only? stats on-change on-cm-init]}]

  (let [value-atom (atom (or initial-value ""))
        on-change  (or on-change (constantly nil))
        cm-inst    (atom nil)
        indexes (when (map? stats) (keys stats))]
    (r/create-class

     {:component-did-mount
      (fn [this]
        (let [el   (r/dom-node this)
              opts #js {:lineNumbers false
                        :undoDepth 100000000
                        :historyEventDelay 1
                        :viewportMargin js/Infinity
                        :autofocus true
                        :readOnly read-only?
                        :value @value-atom
                        :theme "eclipse" ; or "monokai"
                        :autoCloseBrackets true
                        :hintOptions #js {:hint (partial autocomplete indexes)
                                      :completeSingle false}
                        :extraKeys {"Ctrl-Space" "autocomplete"} ;need to leave this in for `:` to work, there's probably a better way!
                        :matchBrackets true
                        :mode "clojure"}
              inst (codemirror. el opts)]
          (.on inst "keyup"
               (fn [cm e] (when (and (not (gobj/getValueByKeys cm #js ["state" "completionActive"]))
                                     (= 1 (-> (gobj/get e "key") (count)))
                                     (= (gobj/get e "key") ":"))
                            (.showHint inst))))
          (reset! cm-inst inst)
          (.on inst "change"
               (fn []
                 (let [value (.getValue inst)]
                   (when-not (= value @value-atom)
                     (on-change value)
                     (reset! value-atom value)))))
          (when on-cm-init
            (on-cm-init inst))))

      :component-did-update
      (fn [this old-argv]
        (when-not (= @value-atom (.getValue @cm-inst))
          (.setValue @cm-inst @value-atom)
          ;; reset the cursor to the end of the text, if the text was changed externally
          (let [last-line (.lastLine @cm-inst)
                last-ch (count (.getLine @cm-inst last-line))]
            (.setCursor @cm-inst last-line last-ch))))

      :reagent-render
      (fn [_ _ _]
        [:div.code-mirror-container
         [:style code-mirror-styling]])})))
