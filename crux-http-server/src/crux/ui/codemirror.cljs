(ns crux.ui.codemirror
  (:require cljsjs.codemirror
            cljsjs.codemirror.mode.clojure
            cljsjs.codemirror.addon.edit.closebrackets
            cljsjs.codemirror.addon.edit.matchbrackets
            cljsjs.codemirror.addon.hint.show-hint
            cljsjs.codemirror.addon.hint.anyword-hint
            cljsjs.codemirror.addon.display.autorefresh
            [reagent.core :as r]
            [reagent.dom :as rd]
            [goog.object :as gobj]))

(defn escape-re [input]
  (let [re (js/RegExp. "([.*+?^=!:${}()|[\\]\\/\\\\])" "g")]
    (-> input str (.replace re "\\$1"))))

(defn fuzzy-re [input]
  (-> (reduce (fn [s c] (str s (escape-re c) ".*")) "" input)
      (js/RegExp "i")))

(def crux-builtin-keywords
 [:find :where :args :rules :offset :limit :order-by
  :timeout :full-results? :crux.db/id])

(defn- autocomplete [index cm options]
  (let [cur (.getCursor cm)
        line (.-line cur)
        ch (.-ch cur)
        token (.getTokenAt cm cur)
        reg (subs (.-string token) 0 (- ch (.-start token)))
        blank? (#{"[" "{" " " "("} reg)
        start (if blank? cur (.Pos js/CodeMirror line (gobj/get token "start")))
        end (if blank? cur (.Pos js/CodeMirror line (gobj/get token "end")))
        words (concat crux-builtin-keywords index)
        fuzzy (if blank? #".*" (fuzzy-re reg))
        words (->> words
                   (map str)
                   (filter #(re-find fuzzy %)))]
    (clj->js {:list words
              :from start
              :to end})))

(defn code-mirror
  [initial-value {:keys [read-only? stats on-change on-blur on-cm-init]}]
  (let [value-atom (atom (or initial-value ""))
        on-change (or on-change (constantly nil))
        cm-inst (atom nil)
        indexes (when (map? stats) (keys stats))]
    (r/create-class
     {:component-did-mount
      (fn [this]
        (let [el (rd/dom-node this)
              opts #js {:lineNumbers false
                        :undoDepth 100000000
                        :historyEventDelay 1
                        :viewportMargin js/Infinity
                        :autoRefresh true
                        :readOnly read-only?
                        :value @value-atom
                        :theme "eclipse"
                        :autoCloseBrackets true
                        :hintOptions #js {:hint (partial autocomplete indexes)
                                                                                  :completeSingle false}
                        :extraKeys {"Ctrl-Space" "autocomplete"}
                        ;; need to leave this in for : to work, there's
                        ;; probably a better way!
                        :matchBrackets true
                        :mode "clojure"}
              inst (js/CodeMirror. el opts)]
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
          (.on inst "blur" (fn [] (on-blur)))
          (when on-cm-init
            (on-cm-init inst))))
      :component-did-update
      (fn []
        (when-not (= @value-atom (.getValue @cm-inst))
          (.setValue @cm-inst @value-atom)
          ;; reset the cursor to the end of the text, if the text was changed
          ;; externally
          (let [last-line (.lastLine @cm-inst)
                last-ch (count (.getLine @cm-inst last-line))]
            (.setCursor @cm-inst last-line last-ch))))
      :reagent-render
      (fn [_ _ _]
        [:div.textarea])})))
