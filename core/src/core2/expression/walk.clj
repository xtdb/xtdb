(ns core2.expression.walk)

(defmulti direct-child-exprs
  (fn [{:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod direct-child-exprs ::default [_] #{})
(defmethod direct-child-exprs :if [{:keys [pred then else]}] [pred then else])
(defmethod direct-child-exprs :if-some [{:keys [local expr then else]}] [expr then else])
(defmethod direct-child-exprs :let [{:keys [expr body]}] [expr body])
(defmethod direct-child-exprs :call [{:keys [args]}] args)

(defn expr-seq [expr]
  (lazy-seq
   (cons expr (mapcat expr-seq (direct-child-exprs expr)))))

(defmulti walk-expr
  (fn [inner outer {:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod walk-expr ::default [_inner outer expr]
  (outer expr))

(defmethod walk-expr :if [inner outer {:keys [pred then else]}]
  (outer {:op :if
          :pred (inner pred)
          :then (inner then)
          :else (inner else)}))

(defmethod walk-expr :if-some [inner outer {:keys [local expr then else]}]
  (outer {:op :if-some
          :local local
          :expr (inner expr)
          :then (inner then)
          :else (inner else)}))

(defmethod walk-expr :let [inner outer {:keys [local expr body]}]
  (outer {:op :let
          :local local
          :expr (inner expr)
          :body (inner body)}))

(defmethod walk-expr :call [inner outer {expr-f :f, :keys [args]}]
  (outer {:op :call
          :f expr-f
          :args (mapv inner args)}))

;; from clojure.walk
(defn postwalk-expr [f expr]
  (walk-expr (partial postwalk-expr f) f expr))

(defn prewalk-expr [f expr]
  (walk-expr (partial prewalk-expr f) identity (f expr)))
